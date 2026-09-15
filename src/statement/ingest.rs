//! Bulk ingest for [`SpannerStatement`]: the `adbc.ingest.*` option parsing, the create/replace
//! table DDL, and the insert-mutation write paths — chunked write-only commits (with the reactive
//! mutation-limit bisect) and the non-atomic BatchWrite firehose.

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
use arrow_array::RecordBatch;
use google_cloud_spanner::mutation::{Mutation, MutationGroup};

use super::{SpannerStatement, string_option};
use crate::bind;
use crate::connection::{TxnKind, lock_txn, write_mutations_txn};
use crate::error::{
    annotate, err, from_spanner, from_status_parts, invalid_state, not_implemented, unsupported,
};
use crate::runtime::block_on_cancellable;
use crate::timeout::with_timeout;

impl SpannerStatement {
    /// DDL to run before an ingest, for the create/replace ingest modes (`None` for append).
    ///
    /// `create` (the default — see [`ingest_mode`](Self::ingest_mode)) builds the table (erroring
    /// if it exists), `create_append` builds it if absent, and `replace` drops any existing table
    /// first. The schema comes from the bound ingest data.
    fn build_ingest_table_ddl(
        &self,
        table: &str,
        mode: Option<IngestMode>,
    ) -> Result<Option<Vec<String>>> {
        // Exhaustive: unknown modes were already rejected by `set_option` (`ingest_mode_option`).
        // Unset (`None`) is `create`, the ADBC spec default.
        let (if_not_exists, drop_first) = match mode {
            // Append into an existing table: no DDL.
            Some(IngestMode::Append) => return Ok(None),
            None | Some(IngestMode::Create) => (false, false),
            Some(IngestMode::CreateAppend) => (true, false),
            Some(IngestMode::Replace) => (false, true),
        };
        let schema = self
            .bound_ingest_schema()
            .ok_or_else(|| invalid_state("cannot create the ingest table: no data is bound"))?;
        let db_schema = self.target_db_schema.as_deref();
        let mut statements = Vec::new();
        if drop_first {
            statements.push(format!(
                "DROP TABLE IF EXISTS {}",
                crate::sql::qualified_table(db_schema, table)
            ));
        }
        statements.push(bind::create_table_sql(
            table,
            db_schema,
            &schema,
            if_not_exists,
        )?);
        Ok(Some(statements))
    }

    /// Whether `table` exists in the ingest target schema (`adbc.ingest.target_db_schema`; empty =
    /// Spanner's default, unnamed schema), via the shared
    /// [`table_exists`](crate::metadata::table_exists) probe. Shared by the two ingest error
    /// remaps below.
    fn ingest_table_exists(&self, table: &str) -> Result<bool> {
        crate::metadata::table_exists(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            self.config.timeouts.query_timeout(),
            self.target_db_schema.as_deref().unwrap_or(""),
            table,
        )
    }

    /// Remap a failed `append`- or `create_append`-mode bulk ingest onto the statuses the ADBC
    /// bulk-ingest contract mandates.
    ///
    /// Both modes insert into a table that may already exist, so the spec wants their insert
    /// failure remapped: for `append` a missing table is [`Status::NotFound`] and a present one is a
    /// schema mismatch ([`Status::AlreadyExists`]); for `create_append` the `CREATE TABLE IF NOT
    /// EXISTS` step guarantees the table is present, so only the schema-mismatch side can surface
    /// (its spec contract: "error if the table exists, but the schema does not match"). `create` and
    /// `replace` keep the raw insert error — their DDL step already owns the table-existence
    /// contract ([`remap_ingest_create_error`](Self::remap_ingest_create_error)).
    ///
    /// A failure that already carries [`Status::AlreadyExists`] — a bound row duplicating a primary
    /// key already in the table, since insert mutations keep `INSERT` semantics — keeps that status
    /// and just gets the target table's name folded into the message. Any other failure is
    /// reinterpreted from the [`ingest_table_exists`](Self::ingest_table_exists) probe; the original
    /// Spanner error's detail is folded into the message.
    fn remap_ingest_append_error(&self, table: &str, error: Error) -> Error {
        if !matches!(
            self.ingest_mode,
            Some(IngestMode::Append) | Some(IngestMode::CreateAppend)
        ) {
            return error;
        }
        // A driver-side transaction-state rejection (ingesting in a manual transaction that began
        // with a query) is not an insert failure, so the spec's NotFound/AlreadyExists
        // append contract does not apply — it propagates unchanged.
        if error.status == Status::InvalidState {
            return error;
        }
        // Already `AlreadyExists`: a duplicate primary key. The status is the one the contract
        // wants — name the target table (consumers key off it) instead of running the exists
        // probe, whose "incompatible schema" wording would misreport a duplicate key.
        if error.status == Status::AlreadyExists {
            // Pure annotation: this branch only names the table, so `annotate` keeps the status,
            // the vendor code and the forwarded `google.rpc.Status` details. (The probe branches
            // below *reinterpret* the error, deriving a new status, so they keep neither.)
            return annotate(error, |message| {
                format!("bulk ingest append into table {table:?} failed: {message}")
            });
        }
        match self.ingest_table_exists(table) {
            Ok(true) => err(
                format!(
                    "bulk ingest append into table {table:?} failed: the bound data is \
                     incompatible with the existing table's schema ({})",
                    error.message
                ),
                Status::AlreadyExists,
            ),
            Ok(false) => err(
                format!(
                    "bulk ingest append target table {table:?} not found ({})",
                    error.message
                ),
                Status::NotFound,
            ),
            // A failed probe teaches us nothing about the table, so the insert error stands (see
            // `table_exists`).
            Err(_) => error,
        }
    }

    /// Run a bulk ingest of the bound rows into `table`, honouring the configured ingest mode.
    ///
    /// Shared by `execute` and `execute_update` so both entry points ingest identically: an ingest
    /// needs no SQL query, so an FFI caller reaches it through either the query out-pointer
    /// (`execute`) or the affected-rows path (`execute_update`). In the create/replace modes the
    /// table is first built (keyless) from the ingest data's Arrow schema via DDL, which Spanner
    /// runs immediately before the inserts. Returns the ingested-row count
    /// (summed across chunk transactions — see [`run_ingest_mutations`](Self::run_ingest_mutations)),
    /// or `None` when the rows were buffered for a manual-transaction commit.
    ///
    /// An ingest small enough for one chunk (the common case) applies atomically; one large enough
    /// to need several chunks does **not** — each chunk commits in its own transaction, so a
    /// mid-ingest failure leaves the earlier chunks' rows committed (the error reports their exact
    /// count — see [`note_rows_already_committed`]).
    pub(super) fn run_ingest(&mut self, table: &str) -> Result<Option<i64>> {
        if self.config.is_read_only() {
            return Err(invalid_state("cannot ingest: the connection is read-only"));
        }
        // An empty (zero-batch) bound stream still declares a schema (`ingest_schema`), which is
        // enough to create the table and commit zero rows — so only a statement with *neither*
        // bound rows nor an ingest schema has genuinely bound nothing.
        if self.bound.is_empty() && self.ingest_schema.is_none() {
            return Err(invalid_state("cannot ingest: no data has been bound"));
        }
        let result = self.run_bound_ingest(table);
        // Consumed by the attempt either way, including a failed create-mode DDL (see
        // `clear_bound`).
        self.clear_bound();
        result
    }

    /// The body of [`run_ingest`](Self::run_ingest), split out so its caller clears the bound data
    /// on every exit path (success, failed DDL, failed insert) in one place.
    fn run_bound_ingest(&self, table: &str) -> Result<Option<i64>> {
        // Reject a DML-kind ingest inside a manual *query* transaction BEFORE any DDL side effect:
        // DDL is not transaction-aware and runs immediately, so a create/replace-mode ingest would
        // otherwise create (or drop) the table before `run_ingest_mutations`'s kind check rejects
        // it. This guard changes no state; the authoritative check still runs under the txn lock at
        // buffer time.
        //
        // It only closes the race for the single-threaded case: a concurrent statement could fix
        // the transaction to query-kind between this check and the DDL, orphaning the table. Fully
        // closing it would mean holding the connection-wide txn lock across a multi-second admin
        // `UpdateDatabaseDdl` RPC — not worth stalling every other statement. The residual window
        // is exactly the documented "DDL is not transaction-aware / DML–DDL reorder" caveat (see
        // CLAUDE.md, `run_ddl`).
        {
            let txn = lock_txn(&self.txn);
            if !txn.autocommit() {
                txn.check_kind_allowed(TxnKind::Dml)?;
            }
        }
        let ingest_ddl = self.build_ingest_table_ddl(table, self.ingest_mode)?;
        if let Some(ddl) = ingest_ddl {
            self.run_ddl(ddl)
                .map_err(|error| self.remap_ingest_create_error(table, error))?;
        }
        // Each remap gates on the ingest mode itself: the DDL failure above is `create`'s to
        // reinterpret, the insert failure below `append`/`create_append`'s.
        self.run_ingest_mutations(table)
            .map_err(|error| self.remap_ingest_append_error(table, error))
    }

    /// Remap a failed `create`-mode ingest DDL onto [`Status::AlreadyExists`] when the target
    /// table already exists.
    ///
    /// `create` mode promises to build the table, so hitting an existing one is the
    /// ADBC-contractual `AlreadyExists` — consumers branch on that status (e.g. to fall back to
    /// append). Spanner reports it as a generic schema-change failure ("Duplicate name in
    /// schema"), so the existence is confirmed via the shared
    /// [`table_exists`](crate::metadata::table_exists) probe and the remapped message names the
    /// table. Only `create` is remapped: `create_append` guards with `IF NOT EXISTS` and `replace`
    /// drops first, so their DDL failures are never about the table already existing. If the table
    /// is absent — or the probe itself fails — the original DDL error surfaces unchanged.
    fn remap_ingest_create_error(&self, table: &str, error: Error) -> Error {
        // Unset (`None`) is `create`, the default, so remap its DDL failure too.
        if !matches!(self.ingest_mode, None | Some(IngestMode::Create)) {
            return error;
        }
        match self.ingest_table_exists(table) {
            Ok(true) => err(
                format!(
                    "bulk ingest create target table {table:?} already exists ({})",
                    error.message
                ),
                Status::AlreadyExists,
            ),
            // An absent table means the DDL failed for some other reason; a failed probe teaches us
            // nothing. Either way the original DDL error stands (see `table_exists`).
            Ok(false) | Err(_) => error,
        }
    }

    /// Ship the bound rows as Spanner **insert mutations**, honouring the connection's transaction
    /// mode and Spanner's per-commit limits.
    ///
    /// Mutations are the `Commit` RPC's native write format: no SQL for Spanner to parse and plan
    /// per row (why they beat per-row `INSERT` DML for bulk loads). Each cell converts through the
    /// same Arrow→Spanner value mapping as parameter binding (see [`bind::insert_mutation`]).
    /// Insert mutations keep `INSERT` semantics — a duplicate primary key fails with
    /// `ALREADY_EXISTS`. (Mutations take no isolation level: Spanner commits blind writes
    /// serializably.)
    ///
    /// **Manual mode** buffers every row's mutation for the next `commit`, which applies them
    /// atomically in the *same* read/write transaction as any buffered DML — Spanner applies
    /// buffered mutations at commit time, after the transaction's DML has executed. Never chunked:
    /// the commit applies the user's whole transaction atomically, so an over-limit manual-mode
    /// ingest fails at commit, as any over-limit user transaction would. Buffering is
    /// **all-or-nothing**: the whole batch is built (outside the transaction lock) before any of
    /// it is buffered, so a row that fails Arrow→Spanner conversion leaves the pending buffer
    /// exactly as it was — a later `commit` never silently applies a partial batch.
    ///
    /// **Autocommit mode** builds and ships the mutations chunk by chunk, each chunk in its own
    /// write-only transaction (with the client's retry/replay protection), returning the ingested
    /// row count summed across chunks. Why chunk: Spanner caps a single commit at ~80,000 mutations
    /// — counted roughly as rows × columns, plus secondary-index entries — and ~100 MB, so one
    /// unchunked commit fails outright once the ingest crosses those cliffs (10k rows × 10 columns
    /// is already there). An ingest that fits [`IngestChunkBudget`]'s conservative budgets still
    /// commits as a single atomic transaction; only one big enough to need several chunks — which
    /// could not have committed as one transaction anyway — loses whole-ingest atomicity, and a
    /// later chunk's failure reports exactly how many rows the earlier chunks committed (see
    /// [`note_rows_already_committed`]). Building per chunk also bounds memory to one chunk of
    /// mutations at a time.
    ///
    /// The `rows × columns` budget cannot see the **secondary-index** entries that also count
    /// toward the per-commit cap, so a heavily-indexed table can overshoot it even inside a
    /// driver-"safe" chunk. As a reactive backstop, a write-only chunk whose commit is rejected for
    /// *too many mutations* is split in half and its halves retried, down to a single row — see
    /// [`write_mutation_range`](Self::write_mutation_range). Like the multi-chunk case, a bisected
    /// chunk is not atomic as a whole; the row count and the already-committed accounting stay
    /// exact. (The BatchWrite path — `spanner.ingest.batch_write` — is not bisected: it ships one
    /// group per row, so the mutation cap does not bind it the same way.)
    fn run_ingest_mutations(&self, table: &str) -> Result<Option<i64>> {
        // Mutations name their target table directly (no SQL quoting; a named schema joins with a
        // plain dot).
        let target = bind::mutation_table(self.target_db_schema.as_deref(), table);
        let manual = {
            let txn = lock_txn(&self.txn);
            if txn.autocommit() {
                false
            } else {
                // An ingest is DML-kind work: a transaction that began with a query rejects it
                // up front, before any mutation-building work is done.
                txn.check_kind_allowed(TxnKind::Dml)?;
                true
            }
        };
        if manual {
            // Manual mode: build *every* row's mutation before touching the buffer, and build
            // outside the txn lock. All-or-nothing buffering keeps the commit contract honest — a
            // mid-row conversion failure (e.g. an out-of-range date) must not strand the rows
            // before it for a later `commit` to apply silently. Keeping the O(rows) build out of
            // the connection-wide mutex also avoids stalling concurrent txn-state users and cannot
            // poison the mutex on a panic.
            let rows = self.bound.iter().map(RecordBatch::num_rows).sum();
            let mutations = self.build_range_mutations(&target, 0, rows)?;
            // An empty append buffers nothing and would commit clean, so a missing target table
            // would never surface — probe existence now (as the autocommit path does, and outside
            // the txn lock) so a manual-mode empty append to an absent table still fails NotFound,
            // consistent with what a non-empty manual append surfaces at commit.
            self.check_empty_append_target(table, rows as i64)?;
            let mut txn = lock_txn(&self.txn);
            if !txn.autocommit() {
                // `buffer_mutation` re-checks the DML kind under this lock (a concurrent
                // statement may have fixed the transaction to queries in the unlocked window);
                // a rejection fails the *first* call, before anything is buffered, so
                // all-or-nothing still holds. Its first success fixes the transaction's kind.
                for mutation in mutations {
                    txn.buffer_mutation(mutation)?;
                }
                return Ok(None);
            }
            // The mode flipped to autocommit while the batch was being built (enabling
            // autocommit commits the manual transaction): fall through to the autocommit path
            // below, exactly where a fresh mode check would have routed this ingest.
        }
        // Autocommit: walk the flattened row sequence (all bound batches concatenated), cutting it
        // into commit chunks by `IngestChunkBudget`. A chunk is a contiguous `[start, end)` range
        // rather than a materialised `Vec<Mutation>`; its mutations are (re)built cheaply from the
        // batches on demand (`commit_ingest_range`), so nothing is cloned up front just to enable
        // the write-only path's bisect-and-retry when a chunk overshoots the per-commit cap.
        let mut total = 0_i64;
        let mut budget = IngestChunkBudget::default();
        let mut chunk_start = 0_usize;
        let mut row_index = 0_usize;
        for batch in &self.bound {
            let columns = batch.num_columns();
            // A cheap per-row size estimate: the batch's Arrow buffer footprint averaged over its
            // rows. Capacity-based, so it slightly over-estimates the wire size — the conservative
            // direction for a budget.
            let row_bytes = batch.get_array_memory_size() / batch.num_rows().max(1);
            for _ in 0..batch.num_rows() {
                if !budget.fits(columns, row_bytes) {
                    total += self.commit_ingest_range(&target, chunk_start, row_index, total)?;
                    budget = IngestChunkBudget::default();
                    chunk_start = row_index;
                }
                budget.add(columns, row_bytes);
                row_index += 1;
            }
        }
        total += self.commit_ingest_range(&target, chunk_start, row_index, total)?;
        self.check_empty_append_target(table, total)?;
        Ok(Some(total))
    }

    /// Surface [`Status::NotFound`] for an **empty** `append` ingest whose target table is absent.
    ///
    /// A zero-row ingest ships nothing, so the insert error that normally drives
    /// [`remap_ingest_append_error`](Self::remap_ingest_append_error)'s NotFound never fires — yet
    /// the ADBC append contract is NotFound for an absent table regardless of row count. Both ingest
    /// paths call this after a zero-row ingest (`ingested == 0`): the autocommit path after its
    /// (no-op) commit, the manual path before buffering nothing. Only `append` needs it —
    /// `create_append`'s `CREATE TABLE IF NOT EXISTS` guarantees the table exists, and
    /// `create`/`replace` own existence via their own DDL. A probe that itself fails teaches nothing
    /// about the table, so — as everywhere else — the empty ingest just succeeds (see
    /// [`table_exists`](crate::metadata::table_exists)).
    fn check_empty_append_target(&self, table: &str, ingested: i64) -> Result<()> {
        if ingested == 0
            && matches!(self.ingest_mode, Some(IngestMode::Append))
            && matches!(self.ingest_table_exists(table), Ok(false))
        {
            return Err(err(
                format!("bulk ingest append target table {table:?} not found"),
                Status::NotFound,
            ));
        }
        Ok(())
    }

    /// Build the insert mutations for the flattened row range `[start, end)` across the bound
    /// batches, mapping each global row index back to its `(batch, row)`.
    ///
    /// The same cheap Arrow→Spanner build the forward path uses ([`bind::insert_mutation`]), so a
    /// bisected retry rebuilds a half's mutations straight from the batches — no `Vec<Mutation>` is
    /// cloned on the happy path solely to keep a copy around for a retry that usually never happens.
    ///
    /// A conversion failure here (e.g. an out-of-range date) on a *later* chunk is annotated by the
    /// autocommit callers with the earlier chunks' committed-row count (COR-6), like a commit
    /// failure, so the `run_ingest` "reports their exact count" contract holds for build errors too.
    fn build_range_mutations(
        &self,
        target: &str,
        start: usize,
        end: usize,
    ) -> Result<Vec<Mutation>> {
        let mut mutations = Vec::with_capacity(end.saturating_sub(start));
        let mut base = 0_usize;
        for batch in &self.bound {
            let rows = batch.num_rows();
            // Intersect the requested global range with this batch's slice of the flattened
            // sequence (`[base, base + rows)`), then translate to batch-local row offsets.
            let lo = start.max(base).saturating_sub(base);
            let hi = end.min(base + rows).saturating_sub(base);
            for row in lo..hi {
                mutations.push(bind::insert_mutation(target, batch, row)?);
            }
            base += rows;
            if base >= end {
                break;
            }
        }
        Ok(mutations)
    }

    /// Commit the flattened row range `[start, end)` as one autocommit ingest chunk, dispatching on
    /// the `spanner.ingest.batch_write` option: the default write-only transaction (with the
    /// reactive mutation-limit bisect — [`write_mutation_range`](Self::write_mutation_range)), or
    /// Spanner's BatchWrite RPC ([`batch_write_chunk`](Self::batch_write_chunk)) for a non-atomic
    /// firehose load. Both return the range's ingested-row count (`0` for an empty range).
    ///
    /// `prior_total` is how many rows this ingest's earlier chunks have already committed; it is
    /// woven into a mid-ingest failure via [`note_rows_already_committed`] so the caller learns the
    /// exact table state.
    fn commit_ingest_range(
        &self,
        target: &str,
        start: usize,
        end: usize,
        prior_total: i64,
    ) -> Result<i64> {
        if start >= end {
            return Ok(0);
        }
        if self.ingest_batch_write {
            // BatchWrite ships one MutationGroup per row and applies groups independently, so the
            // per-commit mutation cap does not bind it the way a write-only `Commit` is bound — it
            // is deliberately left out of the mutation-limit bisect. (Its own per-request size limit
            // could warrant a follow-up, but is not this backstop's concern.)
            let mutations = self
                .build_range_mutations(target, start, end)
                .map_err(|e| note_rows_already_committed(e, prior_total))?;
            self.batch_write_chunk(mutations, prior_total)
        } else {
            self.write_mutation_range(target, start, end, prior_total)
        }
    }

    /// Commit the flattened row range `[start, end)` in one write-only transaction, **splitting it
    /// in half and retrying the two halves** if — and only if — Spanner rejects the commit for
    /// exceeding its per-commit mutation limit.
    ///
    /// The forward path sizes chunks by `rows × columns` mutations ([`IngestChunkBudget`]), but the
    /// *true* commit-time count also includes secondary-index entries the driver cannot see, so a
    /// heavily-indexed table can overshoot Spanner's ~80,000-mutation cap even inside a
    /// driver-"safe" chunk. This is the reactive backstop: on that specific error
    /// ([`is_mutation_limit_exceeded`]) the range is bisected and each half retried, down to a
    /// single row. Every **other** error — a duplicate key, a bad value, a timeout, a cancel, an
    /// `ABORTED` — propagates unchanged, so the append/create remaps and
    /// [`note_rows_already_committed`] still fire. A single row that *still* overshoots is
    /// un-splittable, so its error propagates too (no infinite recursion, no empty commit). Like the
    /// multi-chunk ingest, a bisected chunk is **not atomic as a whole**; `prior_total` is threaded
    /// through the recursion so a mid-bisect failure reports every row committed before it.
    fn write_mutation_range(
        &self,
        target: &str,
        start: usize,
        end: usize,
        prior_total: i64,
    ) -> Result<i64> {
        let mutations = self
            .build_range_mutations(target, start, end)
            .map_err(|e| note_rows_already_committed(e, prior_total))?;
        match self.write_mutation_chunk(mutations) {
            Ok(count) => Ok(count),
            Err(error) if end - start > 1 && is_mutation_limit_exceeded(&error) => {
                let mid = start + (end - start) / 2;
                let left = self.write_mutation_range(target, start, mid, prior_total)?;
                let right = self.write_mutation_range(target, mid, end, prior_total + left)?;
                Ok(left + right)
            }
            Err(error) => Err(note_rows_already_committed(error, prior_total)),
        }
    }

    /// Commit one ingest chunk in its own write-only transaction, returning its row count (`0` for
    /// an empty chunk, which sends nothing).
    ///
    /// Delegates to the shared [`write_mutations_txn`] commit (also the mutations-only
    /// manual-commit path): `WriteOnlyTransaction::write` carries the client's replay protection —
    /// on success the mutations were applied exactly once, retrying internally on `ABORTED`. A
    /// commit reports no affected-row count, but each insert mutation is exactly one row, so the
    /// chunk length is the count.
    fn write_mutation_chunk(&self, mutations: Vec<Mutation>) -> Result<i64> {
        let count = mutations.len() as i64;
        write_mutations_txn(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            &self.config,
            mutations,
        )?;
        Ok(count)
    }

    /// Apply one ingest chunk through Spanner's **BatchWrite** RPC (the
    /// `spanner.ingest.batch_write` autocommit path), returning the number of rows applied.
    ///
    /// Each row's insert mutation is sent as its own [`MutationGroup`]. BatchWrite applies groups
    /// **independently and non-atomically** — the same "not atomic as a whole" guarantee the
    /// multi-chunk write-only path carries, but now within a chunk too — which is what makes it the
    /// cheaper firehose transport. Each streamed [`BatchWriteResponse`] reports, per group index,
    /// whether it applied: an `OK`/absent status counts those rows as applied, and the first
    /// non-`OK` group status — code, message *and* its `google.rpc.Status` details — becomes the
    /// returned error via [`from_status_parts`] (so a duplicate primary key still surfaces as
    /// `AlreadyExists`, its details reach `Error::details`, and the append/create remaps fire,
    /// exactly as on the write-only path). Because a non-atomic batch may have applied some groups
    /// before the failing one — or before a mid-stream transport error — any error is annotated via
    /// [`note_rows_already_committed`] (COR-5), folding this chunk's `applied` groups (one row each)
    /// into `prior_total` so the count covers earlier chunks *and* this chunk's committed rows.
    ///
    /// The request does carry `spanner.request.priority` and `spanner.transaction.tag`
    /// ([`RequestConfig::apply_to_batch_write`](crate::request::RequestConfig::apply_to_batch_write)).
    /// It has no per-request *commit* options and its response has no commit statistics, so
    /// `spanner.commit.max_delay` and `spanner.commit_stats` do not apply on this path; nor does
    /// `spanner.request.tag`, which Spanner ignores for BatchWrite (documented on
    /// [`OPTION_INGEST_BATCH_WRITE`](crate::OPTION_INGEST_BATCH_WRITE)).
    fn batch_write_chunk(&self, mutations: Vec<Mutation>, prior_total: i64) -> Result<i64> {
        if mutations.is_empty() {
            return Ok(0);
        }
        // One mutation group per row: groups are applied independently, so per-row insert failures
        // (e.g. a duplicate key) do not roll back the rest of the chunk.
        let groups: Vec<MutationGroup> = mutations
            .into_iter()
            .map(|m| MutationGroup::new(vec![m]))
            .collect();
        let transaction = self
            .config
            .request
            .apply_to_batch_write(self.client.batch_write_transaction())
            .build();
        block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.update_timeout(),
                crate::OPTION_RPC_TIMEOUT_UPDATE,
                async move {
                    let mut stream = match transaction.execute_streaming(groups).await {
                        Ok(stream) => stream,
                        // Nothing streamed yet, so only earlier chunks are committed.
                        Err(e) => {
                            return Err(note_rows_already_committed(from_spanner(e), prior_total));
                        }
                    };
                    let mut applied = 0_i64;
                    let mut first_error: Option<Error> = None;
                    while let Some(response) = stream.next().await {
                        // A mid-stream transport error: the groups reported OK so far stay
                        // committed (BatchWrite is non-atomic), so fold `applied` into the count.
                        let response = match response {
                            Ok(response) => response,
                            Err(e) => {
                                return Err(note_rows_already_committed(
                                    from_spanner(e),
                                    prior_total + applied,
                                ));
                            }
                        };
                        // An `OK` (or absent) status means the referenced groups applied; any other
                        // status marks them failed — capture the first such failure to return.
                        match response.status.as_ref().filter(|s| s.code != 0) {
                            None => applied += response.indexes.len() as i64,
                            Some(status) if first_error.is_none() => {
                                first_error = Some(from_status_parts(
                                    status.code,
                                    &status.message,
                                    &status.details,
                                ));
                            }
                            Some(_) => {}
                        }
                    }
                    match first_error {
                        // A failing group is non-atomic with the rest, so the groups that did apply
                        // (this chunk's `applied`, plus earlier chunks) are folded into the count.
                        Some(error) => {
                            Err(note_rows_already_committed(error, prior_total + applied))
                        }
                        None => Ok(applied),
                    }
                },
            ),
        )
    }
}

/// Validate the `adbc.ingest.target_catalog` option. Spanner has a single, unnamed (`""`) catalog,
/// so only the empty catalog is accepted; any other name is rejected as unsupported.
pub(super) fn check_target_catalog(catalog: String) -> Result<String> {
    if catalog.is_empty() {
        Ok(catalog)
    } else {
        Err(unsupported(format!(
            "ingest target catalog {catalog:?}: Spanner has only the default, unnamed catalog; \
             set adbc.ingest.target_catalog to \"\" or leave it unset"
        )))
    }
}

/// Parse the `adbc.ingest.mode` option into an [`IngestMode`], accepting both the spec's canonical
/// `adbc.ingest.mode.*` spellings and the bare short forms (`append`, `create`, …). Unknown modes
/// are rejected here — at `set_option` time — which is what lets the ingest paths
/// ([`SpannerStatement::build_ingest_table_ddl`]) match the enum exhaustively, with no fallback arm
/// to drift.
pub(super) fn ingest_mode_option(key: &OptionStatement, value: OptionValue) -> Result<IngestMode> {
    use adbc_core::constants::{
        ADBC_INGEST_OPTION_MODE_APPEND, ADBC_INGEST_OPTION_MODE_CREATE,
        ADBC_INGEST_OPTION_MODE_CREATE_APPEND, ADBC_INGEST_OPTION_MODE_REPLACE,
    };
    match string_option(key, value)?.as_str() {
        ADBC_INGEST_OPTION_MODE_APPEND | "append" => Ok(IngestMode::Append),
        ADBC_INGEST_OPTION_MODE_CREATE | "create" => Ok(IngestMode::Create),
        ADBC_INGEST_OPTION_MODE_CREATE_APPEND | "create_append" => Ok(IngestMode::CreateAppend),
        ADBC_INGEST_OPTION_MODE_REPLACE | "replace" => Ok(IngestMode::Replace),
        other => Err(not_implemented(&format!("ingest mode {other:?}"))),
    }
}

/// Parse the `spanner.ingest.batch_write` statement option. Like the driver's other unset-able
/// booleans (`spanner.commit_stats`), an empty/whitespace string unsets it (back to `false`, the
/// write-only-transaction path); otherwise it is a boolean string (exactly `true`/`false`).
pub(super) fn ingest_batch_write_option(value: OptionValue) -> Result<bool> {
    crate::options::bool_option_unsettable(
        value,
        &format!("option {}", crate::OPTION_INGEST_BATCH_WRITE),
    )
}

/// Annotate a failed autocommit ingest commit with the number of rows already committed and left in
/// the table.
///
/// Each chunk commits in its own transaction (see
/// [`SpannerStatement::run_ingest_mutations`]), so a mid-ingest failure leaves the earlier chunks'
/// rows in the table. On the write-only path a chunk is atomic, so `committed` is just the earlier
/// chunks; on the non-atomic BatchWrite path it also includes the failing chunk's groups that did
/// apply (COR-5). Either way that count is known exactly, and reporting it tells the caller what
/// state the table was left in instead of making them guess.
///
/// A [`Status::Timeout`]/[`Status::Cancelled`] failure is the exception (CON-5): cancel/timeout
/// *drops* the in-flight `Commit` future, which may still land server-side, so the **failing
/// chunk's own** outcome is unknown — a caller-driven retry could duplicate its rows. There the
/// exact count still covers the earlier work, but the annotation also flags the ambiguity rather
/// than implying the failing chunk committed nothing. Other statuses keep the plain accounting; a
/// first-chunk failure with a known outcome (nothing committed) passes through unchanged.
/// The status and `vendor_code` are preserved, so callers still branch on the underlying failure
/// (e.g. `AlreadyExists` for a duplicate primary key).
fn note_rows_already_committed(error: Error, committed: i64) -> Error {
    let outcome_unknown = matches!(error.status, Status::Timeout | Status::Cancelled);
    if committed == 0 && !outcome_unknown {
        return error;
    }
    let mut note = String::new();
    if committed > 0 {
        note.push_str(&format!(
            "{committed} row(s) from this bulk ingest were already committed and remain in the \
             table"
        ));
    }
    if outcome_unknown {
        if !note.is_empty() {
            note.push_str("; ");
        }
        note.push_str(
            "this chunk's own commit outcome is unknown — it may still have landed server-side, so \
             retrying could duplicate rows",
        );
    }
    // Pure annotation, like the append remap's `AlreadyExists` branch: status, vendor code and
    // forwarded `google.rpc.Status` details all survive the rewritten message.
    annotate(error, |message| format!("{message} ({note})"))
}

/// Whether `error` is Spanner's specific "this commit has too many mutations" rejection — the one
/// error the autocommit bulk-ingest write-only path treats as recoverable by splitting the failing
/// chunk and retrying its halves (see [`SpannerStatement::write_mutation_range`]).
///
/// Deliberately narrow. Spanner reports the per-commit mutation-count limit as an `INVALID_ARGUMENT`
/// reading "The transaction contains too many mutations. …Please reduce the number of writes, or use
/// fewer indexes. (Maximum number: N)" — a phrasing stable across the successive 20k→40k→80k limit
/// bumps that names the exact cause (index entries) this backstop targets. Matching on
/// [`Status::InvalidArguments`] **and** that anchor phrase keeps any *other* `INVALID_ARGUMENT` — a
/// malformed value, a schema mismatch — from being silently bisected; those must keep propagating so
/// the ingest append/create remaps and [`note_rows_already_committed`] still fire. The companion
/// commit-size limit (~100 MB / the gRPC request-size cap) is intentionally **not** matched: the byte
/// budget ([`INGEST_CHUNK_BYTE_BUDGET`]) already keeps chunks well under it, and its "request too
/// large" wording is far less stable.
fn is_mutation_limit_exceeded(error: &Error) -> bool {
    error.status == Status::InvalidArguments
        && error
            .message
            .to_ascii_lowercase()
            .contains("too many mutations")
}

/// Per-chunk mutation budget for bulk ingest. Spanner caps a single commit at ~80,000 mutations,
/// and an insert mutation counts roughly its column count **plus** secondary-index entries the
/// driver cannot see, so the budget stays at a quarter of the cap to leave headroom for indexed
/// tables.
const INGEST_CHUNK_MUTATION_LIMIT: u64 = 20_000;

/// Per-chunk approximate byte budget for bulk ingest: well under both Spanner's ~100 MB commit cap
/// and typical gRPC request-size limits (~10 MB), with headroom because the per-row estimate
/// ([`IngestChunkBudget`]) is approximate.
const INGEST_CHUNK_BYTE_BUDGET: u64 = 4 * 1024 * 1024;

/// Budgets the rows of one bulk-ingest commit chunk against Spanner's per-commit limits.
///
/// Each row costs its column count in mutations and an approximate byte size; a chunk is cut when
/// the next row no longer [`fits`](Self::fits) under [`INGEST_CHUNK_MUTATION_LIMIT`] and
/// [`INGEST_CHUNK_BYTE_BUDGET`]. Pure arithmetic — unit-tested offline below.
#[derive(Default)]
struct IngestChunkBudget {
    rows: u64,
    mutations: u64,
    bytes: u64,
}

impl IngestChunkBudget {
    /// Whether a `columns`-wide row of approximately `row_bytes` bytes still fits in the current
    /// chunk. The first row always fits, so a single row larger than the whole budget still forms
    /// its own one-row chunk (never an empty chunk or an infinite loop).
    fn fits(&self, columns: usize, row_bytes: usize) -> bool {
        self.rows == 0
            || (self.mutations.saturating_add(columns as u64) <= INGEST_CHUNK_MUTATION_LIMIT
                && self.bytes.saturating_add(row_bytes as u64) <= INGEST_CHUNK_BYTE_BUDGET)
    }

    /// Record a row as added to the current chunk.
    fn add(&mut self, columns: usize, row_bytes: usize) {
        self.rows += 1;
        self.mutations = self.mutations.saturating_add(columns as u64);
        self.bytes = self.bytes.saturating_add(row_bytes as u64);
    }
}

#[cfg(test)]
mod tests;
