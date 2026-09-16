# dbt on Spanner: design sketch

This repository has no dbt adapter. This sketch proposes materializations using the Python
ADBC driver; the SQL is illustrative and has not been validated as an adapter. Driver behavior
is documented in [transactions](transactions.md) and [options](options.md).

## Connection and transaction model

A future adapter could wrap the [Python DBAPI interface](../python/README.md):

```python
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import DatabaseOptions

conn = spanner.connect(
    db_kwargs={
        DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d",
    },
    autocommit=True,
)
```

Credentials default to ADC; explicit credentials use the `spanner.auth.*` options. Python DBAPI
defaults to `autocommit=False`, so an adapter must enable autocommit explicitly for this strategy.

Manual transactions accept queries or buffered writes, with no read-your-writes. DDL executes
immediately through the admin API, outside the transaction. Autocommit lets each completed DML
statement's changes become visible to the next statement; it does not make a materialization atomic.

## Table materialization

GoogleSQL's [CREATE TABLE syntax][ddl] has no `AS SELECT` form. Create a staging table, populate it
with DML, and publish it through one [atomic table rename][rename]:

```sql
CREATE TABLE `my_model__dbt_tmp` (
  `id` INT64 NOT NULL,
  `name` STRING(MAX),
  `amount` NUMERIC
) PRIMARY KEY (`id`);

INSERT INTO `my_model__dbt_tmp` (`id`, `name`, `amount`)
SELECT `id`, `name`, `amount` FROM ( /* compiled model SQL */ );

-- When my_model already exists:
RENAME TABLE `my_model` TO `my_model__dbt_backup`,
             `my_model__dbt_tmp` TO `my_model`;

-- After checking dependencies on the displaced table:
DROP TABLE `my_model__dbt_backup`;
```

On the first run, rename only `my_model__dbt_tmp` to `my_model`. The adapter must also handle
leftover staging/backup tables and concurrent runs.

- Get the compiled query's output schema through the driver's `execute_schema` plan probe and map
  Arrow fields to [GoogleSQL types][ddl]. Declare the model's configured primary key explicitly.
- Build required indexes and constraints on the replacement table before publishing it. Renaming
  preserves references to the original table: foreign keys, interleaved children, change streams,
  and permissions do not automatically move to the replacement. Views need separate review.
  See [table renaming behavior][rename].
- Check [schema dependencies][schema-updates] before deleting the backup; indexes and interleaved
  children can prevent deletion. This strategy needs additional design for dependent tables.
- A single `INSERT … SELECT` must fit Spanner's transaction limits. Larger loads need chunking or
  bulk ingest into the staging table. [Partitioned DML][partitioned-dml] supports `UPDATE` and
  `DELETE`, not `INSERT`.

## Incremental materialization

For primary-key upserts, use one autocommitted [INSERT OR UPDATE][dml]:

```sql
INSERT OR UPDATE INTO `my_model` (`id`, `payload`, `updated_at`)
SELECT `id`, `payload`, `updated_at`
FROM ( /* compiled model SQL filtered to new or changed rows */ );
```

The configured `unique_key` must match the table's primary key, and the input should contain one
row per key. Only listed columns are overwritten. `ON CONFLICT … DO UPDATE` supports an explicit
primary-key or unique-index conflict target and a conditional `WHERE`; GoogleSQL has no `MERGE`.
See the [DML reference][dml].

A delete-and-insert strategy should stage its input once. If the delete and insert each autocommit,
a failure between them leaves missing rows. An adapter must define recovery or use a table rebuild
when atomic publication is required.

Schema-change handling should compare the target's `get_table_schema` result with the model's
`execute_schema` result. Add or drop columns through DDL where supported; incompatible changes
may require rebuilding. Spanner supports some [in-place type changes][schema-updates], so these
must be checked by type rather than rejected universally.

## Snapshots

An SCD Type 2 snapshot needs to close previous versions and insert new ones. Autocommitting those
steps exposes intermediate states, so an adapter must define recovery and concurrency behavior.
Stage the source once, assign a distinct identifier to each historical version, and preserve dbt's
chosen snapshot strategy semantics. Hashing only the key and tracked values is insufficient when a
row changes back to an earlier value. No snapshot implementation is provided here.

## Seeds and bulk ingest

Use `cur.adbc_ingest(table, arrow_data, mode=...)` for CSV seeds converted to Arrow:

- `create`, `create_append`, and `replace` can create a table from the Arrow schema. They omit an
  explicit primary key; Spanner provides a hidden `rowid`. Pre-create a keyed table and use
  `append` when the seed requires a key.
- Autocommit ingest commits chunks separately. Failure can leave part of a seed written; use a
  staging table and a rename when publication must be atomic.
- `spanner.ingest.batch_write=true` uses BatchWrite with one atomic group per row. A chunk is not
  atomic across its groups; see [transaction behavior](transactions.md).

## Profile and model options

Map connection credentials and the database URI into the adapter profile. Useful optional settings
include request priority, request/transaction tags, RPC timeouts, and retry bounds. Read staleness
applies to read-only queries; it does not make the source of an `INSERT … SELECT` stale.
See the [option reference](options.md) for keys, levels, and defaults.

[ddl]: https://cloud.google.com/spanner/docs/reference/standard-sql/data-definition-language
[dml]: https://cloud.google.com/spanner/docs/reference/standard-sql/dml-syntax
[rename]: https://cloud.google.com/spanner/docs/table-name-synonym
[schema-updates]: https://cloud.google.com/spanner/docs/schema-updates
[partitioned-dml]: https://cloud.google.com/spanner/docs/dml-partitioned
