# dbt on Spanner: an autocommit materialization strategy (design sketch)

> **Status: forward-looking design sketch — not an implemented or official adapter.** There is no
> `dbt-spanner` adapter in this repository (or anywhere) today. This page reasons about *how* a future
> [dbt](https://docs.getdbt.com/) adapter built on top of the `adbc-spanner` driver would implement
> its core materializations, and why the design is anchored on **autocommit** rather than
> multi-statement transactions. It is written to be correct about Cloud Spanner's SQL/DDL semantics
> and about how *this* driver actually behaves — not as generic dbt boilerplate. Everything below is a
> proposal for discussion, not a supported feature. See the repository
> [README](../README.md) for the driver's actual capabilities.

## Why this needs its own strategy

A dbt adapter turns a `SELECT` (the model's compiled SQL) into a persisted relation, and re-runs are
expected to be idempotent. The reference adapters lean on two things Spanner does **not** provide:

1. **`CREATE TABLE AS SELECT` (CTAS).** Spanner has no CTAS. A table must be created by a `CREATE
   TABLE` [schema change](https://cloud.google.com/spanner/docs/schema-updates) (DDL) and then
   populated by a separate `INSERT … SELECT` (DML). Building a relation is inherently a two-step,
   DDL-then-DML operation, never one statement.
2. **Transactional DDL.** Spanner DDL is **not** transactional: it goes through the admin
   `UpdateDatabaseDdl` API and each schema change auto-commits on its own. A `;`-separated DDL batch
   is applied [near-atomically](https://cloud.google.com/spanner/docs/schema-updates) but is *not*
   truly atomic, and DDL can never be enrolled in a data (read/write) transaction alongside DML.

So the classic "wrap the whole build in one transaction and roll back on failure" pattern is
unavailable at the SQL layer regardless of the client. On top of that, this driver's manual
(`autocommit=false`) transaction mode has semantics that make wrapping a multi-step materialization
in one transaction actively wrong (below). The conclusion — matching how
[`dbt-bigquery`](https://docs.getdbt.com/reference/resource-configs/bigquery-configs) operates on a
similarly non-transactional-DDL warehouse — is to run the adapter in **autocommit** and achieve
atomicity/idempotency through an **atomic table-rename swap** rather than a long transaction.

## Why autocommit, concretely (not manual transactions)

This driver defaults to autocommit; setting `adbc.connection.autocommit=false` enters a manual mode
that **buffers** DML and applies it in one read/write transaction at `commit`. For a dbt
materialization, that mode is the wrong tool, for three independent reasons:

- **One kind of work per transaction.** A manual transaction is exactly one of **queries, DML, or
  DDL**, fixed by its first statement; a statement of any other kind fails with `InvalidState`
  until `commit`/`rollback`. A build that interleaves DDL and DML — which every table
  materialization does — therefore cannot run inside one manual transaction at all.
- **No read-your-writes.** In a DML transaction the writes are *buffered* until commit, so a query
  could never see them. An `INSERT … SELECT` into a staging table followed by a `SELECT`/`MERGE`
  reading that staging table would read the *pre-insert* state — so rather than silently returning
  that stale snapshot, the driver **rejects** the query (the kind-mixing rule above), failing
  loudly with `InvalidState` ("commit or roll back first"). A materialization that assumes it can
  read a table it just wrote in the same open transaction fails outright instead of being quietly
  wrong — but it still cannot work as written.
- **DDL commits are only near-atomic.** A DDL manual transaction buffers its statements and
  applies them on commit as one `UpdateDatabaseDdl` batch — applied *in order*, so a mid-batch
  failure leaves the earlier statements applied ("roll back the transaction" cannot undo a
  half-applied batch, and Spanner DDL is never transactional to begin with).

Autocommit sidesteps all of this: **each statement commits before the next runs**, so a staging
table that step *N* writes is fully visible to step *N+1*, and the sequence reads exactly like
ordinary sequential SQL. The price — no cross-statement atomicity — is paid back by structuring
every materialization so that the *only* operation that makes the new data live is a single atomic
step: an `ALTER TABLE … RENAME`.

> **Do not do this:** do not set `adbc.connection.autocommit=false` around a multi-step
> materialization expecting to read intermediate writes back. A manual transaction is one kind of
> work (queries, DML, or DDL) — the first DDL/DML statement of the build fixes the kind, and the
> next statement of another kind **fails with `InvalidState`**. Use autocommit + a rename swap
> instead.

## Spanner SQL dialect notes the adapter must honour

These constrain every generated statement:

- **Every table needs an explicit `PRIMARY KEY`.** Spanner has no implicit `rowid`/`oid`. The adapter
  must derive a key for every model it creates — from a configured `unique_key`, or a surrogate. A
  keyless model is not expressible.
- **Backtick identifier quoting.** Identifiers are quoted with backticks: `` `my_model` ``.
- **[GoogleSQL types](https://cloud.google.com/spanner/docs/data-types).** `INT64`, `FLOAT64`,
  `NUMERIC`, `BOOL`, `STRING(MAX)` / `STRING(n)`, `BYTES(MAX)`, `DATE`, `TIMESTAMP`, `JSON`,
  `ARRAY<…>`. There is no `INT`/`VARCHAR`/`TEXT`; column definitions in generated DDL must use the
  GoogleSQL spellings.
- **`INSERT … SELECT` for population**, `MERGE` for upserts, `DELETE`/`UPDATE` for the rest — all
  ordinary DML the driver runs through its read/write path.

## Table materialization

Full-refresh build of a model as a physical table. Because there is no CTAS, build into a temporary
name, populate it, then swap atomically:

```sql
-- 1. DDL: create the temp table. Every table needs a PRIMARY KEY.
CREATE TABLE `my_model__dbt_tmp` (
  `id`         INT64        NOT NULL,
  `name`       STRING(MAX),
  `amount`     NUMERIC,
  `updated_at` TIMESTAMP,
) PRIMARY KEY (`id`);

-- 2. DML: populate from the model's compiled SELECT (autocommitted).
INSERT INTO `my_model__dbt_tmp` (`id`, `name`, `amount`, `updated_at`)
SELECT `id`, `name`, `amount`, `updated_at`
FROM ( /* the model's compiled SQL */ );

-- 3. Atomic swap (DDL): drop the old, rename temp into place.
DROP TABLE IF EXISTS `my_model`;
ALTER TABLE `my_model__dbt_tmp` RENAME TO `my_model`;
```

Notes and honest caveats:

- The column list and types in step 1 come from the model's schema. The adapter can obtain it from a
  `QueryMode::Plan` probe of the compiled SQL — surfaced by the driver's `execute_schema` — mapping
  each Arrow field back to its GoogleSQL type, plus the model's configured `primary_key`. There is no
  server-side "create a table shaped like this query" shortcut.
- Steps 1 and 3 are DDL and each auto-commits; step 2 is DML that autocommits on its own. The window
  in which the model does not exist is the `DROP` + `RENAME` pair. Spanner applies a `;`-batched DDL
  group near-atomically, so issuing the `DROP` and `RENAME` as one batch narrows — but does not fully
  close — that window; there is no truly-atomic cross-DDL swap. This is inherent to Spanner, not to
  the driver.
- Large `INSERT … SELECT` volumes are subject to Spanner's per-transaction
  [mutation limits](https://cloud.google.com/spanner/quotas). Very large full refreshes may need
  partitioned DML or chunking, which the adapter would generate rather than a single statement.

## Incremental materialization

Append/upsert only the new or changed rows into an existing target. Build a **committed** staging
table (visible because autocommitted), then merge from it into the target keyed on `unique_key`:

```sql
-- 1. Stage the incremental slice into a committed temp table.
CREATE TABLE `my_model__dbt_tmp` (
  `id`         INT64 NOT NULL,
  `payload`    STRING(MAX),
  `updated_at` TIMESTAMP,
) PRIMARY KEY (`id`);

INSERT INTO `my_model__dbt_tmp` (`id`, `payload`, `updated_at`)
SELECT `id`, `payload`, `updated_at`
FROM ( /* the model's compiled SQL, filtered by its is_incremental() predicate */ );

-- 2. MERGE staging into the target on unique_key (a complete, autocommitted statement).
MERGE INTO `my_model` AS t
USING `my_model__dbt_tmp` AS s
ON t.`id` = s.`id`
WHEN MATCHED THEN
  UPDATE SET t.`payload` = s.`payload`, t.`updated_at` = s.`updated_at`
WHEN NOT MATCHED THEN
  INSERT (`id`, `payload`, `updated_at`) VALUES (s.`id`, s.`payload`, s.`updated_at`);

-- 3. Clean up the staging table.
DROP TABLE IF EXISTS `my_model__dbt_tmp`;
```

Because staging is autocommitted before the `MERGE` runs, step 2 reads it correctly — the exact
behaviour manual mode would break. When a composite `unique_key` is configured, the `ON` clause ANDs
each key column. If `MERGE` is undesirable (e.g. a delete-then-insert `incremental_strategy`), the
same shape works with `DELETE FROM target WHERE id IN (SELECT id FROM staging)` followed by
`INSERT … SELECT` from staging — again each step autocommitted and individually complete.

**On-schema-change.** dbt's `on_schema_change` needs column-set reconciliation between the model and
the existing target. Because Spanner alters columns via DDL (`ALTER TABLE … ADD COLUMN`, one column
at a time, auto-committing), `append_new_columns` / `sync_all_columns` map to generated `ADD
COLUMN` / `DROP COLUMN` DDL run before the merge; `fail` compares the two schemas (via
`get_table_schema` on the target vs. the model's `execute_schema` probe) and aborts on drift.
Spanner has no cheap in-place type change, so incompatible type changes realistically fall back to a
full rebuild (the table strategy above).

## Snapshot materialization (SCD Type 2)

Snapshots track history: each source row version is a row with `dbt_valid_from` / `dbt_valid_to`
validity bounds and a `dbt_scd_id` surrogate. The whole update is expressible as `MERGE` statements
against the snapshot table, each a **complete, autocommitted statement** that reads both the snapshot
target and the source — no read-your-writes dependency across statements, so autocommit is exactly
right.

```sql
-- Close out rows whose tracked columns changed or that disappeared from the source:
MERGE INTO `my_snapshot` AS t
USING ( /* source, with a computed dbt_scd_id hash of the key + tracked columns */ ) AS s
ON t.`dbt_scd_id` = s.`dbt_scd_id`
WHEN MATCHED AND t.`dbt_valid_to` IS NULL
             AND t.`dbt_change_signature` <> s.`dbt_change_signature`
  THEN UPDATE SET t.`dbt_valid_to` = s.`snapshot_ts`;

-- Insert the new versions (and brand-new rows) as open-ended records:
INSERT INTO `my_snapshot` (`dbt_scd_id`, /* … business columns … */,
                           `dbt_valid_from`, `dbt_valid_to`)
SELECT s.`dbt_scd_id`, /* … */, s.`snapshot_ts`, NULL
FROM ( /* source */ ) AS s
LEFT JOIN `my_snapshot` AS t
  ON t.`dbt_scd_id` = s.`dbt_scd_id` AND t.`dbt_valid_to` IS NULL
WHERE t.`dbt_scd_id` IS NULL;
```

Key points:

- `dbt_scd_id` is the natural **`PRIMARY KEY`** for the snapshot table — Spanner needs one, and a
  stable surrogate over the source key (plus the snapshot timestamp for the versioned form) supplies
  it. The `check` strategy hashes the tracked columns into a change signature; the `timestamp`
  strategy compares an updated-at column instead.
- Each `MERGE`/`INSERT` is one autocommitted statement, so the snapshot table it reads is always its
  committed state. Ordering matters (close-out before insert), and because each step commits, a
  failure between them leaves a well-defined, inspectable intermediate state rather than a torn
  transaction — re-running the snapshot converges.

## Seeds → bulk ingest

dbt `seed` loads a CSV as a table. This maps directly onto the driver's
[bulk-ingest](../README.md#status) path rather than row-by-row `INSERT` DML: the adapter would set
`adbc.ingest.target_table` and stream the seed as an Arrow table, which the driver writes as native
**insert mutations** (not per-row parsed SQL). Relevant knobs (full list in
[docs/options.md](options.md#statement-options)):

- **`adbc.ingest.mode`** — `create` / `create_append` / `replace` build the table from the seed's
  Arrow schema; `append` adds to an existing one.
- **Primary key.** A create mode adds a synthetic `adbc_ingest_key` `STRING` UUID primary key unless
  **`spanner.ingest.primary_key`** names one or more existing seed columns to key on (in key order).
  A dbt seed config that declares a primary key would set this so no synthetic column appears.
- **`spanner.ingest.batch_write`** — for large seeds, route each autocommit chunk through Spanner's
  BatchWrite RPC (non-atomic, higher throughput). Chunking, insert semantics and the row count are
  preserved.

A large seed commits chunk-by-chunk under Spanner's per-commit mutation limits and so is **not
atomic as a whole** (a mid-load failure leaves earlier chunks committed) — consistent with dbt's
expectation that a seed is a full rebuild of a static table, and with the table-swap approach above
if atomicity of the visible relation is required (ingest into a temp name, then rename).

## Connectivity and useful profile options

The adapter would drive the driver from Python through the
[`adbc-driver-spanner`](../python/README.md) package plus `adbc_driver_manager`, which exposes the
standard **DBAPI 2.0** connection/cursor surface (and Arrow result fetching) that a dbt adapter's
connection manager wraps. A `profiles.yml` entry would carry the database path and credentials as
driver options — the same `spanner.auth.*` keys the driver already documents (e.g.
`spanner.auth.keyfile`, `spanner.auth.impersonate.target_principal`, `spanner.auth.quota_project`),
so a dbt profile maps cleanly onto them.

Beyond credentials, several driver options are directly useful in a dbt profile or per-model config
(all in [docs/options.md](options.md)):

- **`spanner.read.staleness`** — a bounded/exact stale read (e.g. `max:10s`, `exact:30s`) makes
  read-only model queries cheaper and lock-free. Ideal on non-critical / analytics models where
  slightly stale input is acceptable; leave it unset (strong read) where freshness matters. Settable
  per statement, so it can be a per-model knob.
- **`spanner.request.priority`** / **`spanner.request.tag`** / **`spanner.transaction.tag`** — tag dbt
  runs (`request.tag = dbt-run-<invocation_id>`) for
  [troubleshooting-with-tags](https://cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags)
  visibility, and drop batch/backfill models to `low` priority so they yield to serving traffic.
- **`spanner.ingest.*`** — the seed knobs above.
- **`spanner.rpc.timeout_seconds.*`** / **`spanner.retry.*`** — bound long-running model builds and
  tune retry behaviour for flaky-network CI runs.

## Summary

Spanner's lack of CTAS and its non-transactional DDL mean a dbt adapter cannot lean on
transaction-wrapped builds, and this driver's manual mode (one kind of work per transaction —
queries, DML, or DDL — with buffered DML/DDL and no read-your-writes, so a read-back or a mixed
DDL/DML sequence fails loudly with `InvalidState`) makes wrapping a multi-step materialization in
`autocommit=false` actively incorrect. The
workable design — mirroring `dbt-bigquery` — runs entirely in **autocommit**, so each staged
statement is committed and visible to the next, and derives atomicity/idempotency from an **atomic
`ALTER TABLE … RENAME` swap** for tables and from **individually-complete `MERGE` statements** for
incremental and snapshot models. Seeds ride the driver's native bulk-ingest path. None of this is
implemented yet; this page is the design rationale for a future adapter.
