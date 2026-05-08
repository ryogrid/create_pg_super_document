# Appendix B — Glossary

[← Symbol Index](appendix_symbol_index.md) | [index](index.md) | [next: Data Structures →](appendix_data_structures.md)

---

Recovery-specific terminology used throughout this document.

| Term | Definition |
|------|-----------|
| **Archive recovery** | Recovery variant that uses `restore_command` to fetch WAL segments and timeline-history files from a third-party archive. Triggered by the presence of `recovery.signal`. Subsumed by **standby mode** when `standby.signal` is also present. |
| **`backup_label`** | A file written by `pg_basebackup` / `pg_backup_start` that records the redo start LSN of a base backup. The file is *named* `backup_label` (no other suffix); say "the backup_label" rather than "the backup label file". |
| **`BLK_DONE`** | One return value of `XLogReadBufferForRedo`. Means `page->pd_lsn >= record_lsn` so the page is already past this record; the redo callback skips. |
| **`BLK_NEEDS_REDO`** | Return value of `XLogReadBufferForRedo` when the caller must apply the record to the page. |
| **`BLK_NOTFOUND`** | Return value when the page or relation no longer exists. |
| **`BLK_RESTORED`** | Return value when the record carried a Full-Page Image and the buffer was filled from the FPI. |
| **Cascade replication** | A standby with its own walsenders feeding downstream standbys. Triggered by `WalSndWakeupProcessRequests` from `ApplyWalRecord`. |
| **Checkpoint** | A primary-side operation that flushes buffers and SLRUs and emits a `XLOG_CHECKPOINT_*` WAL record. The standby analogue is a **restartpoint**. |
| **Consistency point** | Synonym for `minRecoveryPoint`. The LSN at/past which the on-disk image is a consistent transactional snapshot. Never use "convergence point". |
| **Continuous recovery** | The hot-standby flavor of recovery: redo loop never terminates on EOF; only on promote signal. |
| **Crash recovery** | Recovery variant with no signal files. The driver consumes only `pg_wal/`, replays from the last checkpoint, terminates on EOF. |
| **DBState** | The `state` field of `pg_control` (`DB_STARTUP`, `DB_SHUTDOWNED`, `DB_SHUTDOWNED_IN_RECOVERY`, `DB_SHUTDOWNING`, `DB_IN_CRASH_RECOVERY`, `DB_IN_ARCHIVE_RECOVERY`, `DB_IN_PRODUCTION`). |
| **EOF** | End of WAL stream. In crash recovery: end of redo. In archive: try archive then end. In standby: wait for more WAL. |
| **Failover** | Cluster-level operation that promotes a standby and redirects clients (DNS, load balancer, virtual IP). Distinct from **promotion**, which is the standby-side action. |
| **FPI** | Full-Page Image. A WAL record may include a copy of an entire page; replaying the record overwrites the page from the FPI bytes (idempotent regardless of page LSN). Triggered by `full_page_writes=on`. |
| **GXACT** | Global transaction (prepared transaction) entry in `TwoPhaseState` shmem. |
| **Hot standby** | A recovering cluster that accepts read-only queries (`hot_standby = on` and `standbyState == SNAPSHOT_READY`). Distinct from **warm standby**, which accepts no queries. |
| **`KnownAssignedXids`** | Standby-side mirror of the primary's procarray. A sorted ring buffer in shmem, populated from `XLOG_RUNNING_XACTS` and maintained per WAL record. |
| **LSN** | Log Sequence Number. A 64-bit byte offset into the WAL stream. `XLogRecPtr`. |
| **`minRecoveryPoint`** | The consistency point. Stored in `pg_control`. Once `lastReplayedEndRecPtr >= minRecoveryPoint && backupEndPoint passed`, the cluster is safe to read. |
| **Overflowed RUNNING_XACTS** | An `xl_running_xacts` record with `subxid_overflow=true`, meaning the primary had more than `PGPROC_MAX_CACHED_SUBXIDS` subxids and could not list them all. The standby must wait for a non-overflowed record before opening hot standby. |
| **PITR** | Point-in-time recovery. The use case for `recovery_target_*` GUCs. |
| **`pg_control`** | The control file at `$PGDATA/global/pg_control`. Stores the cluster's state, last checkpoint LSN, `minRecoveryPoint`, etc. Read by `ReadControlFile`, written by `UpdateControlFile`. CRC-protected. |
| **PMState** | The postmaster's state machine: `PM_INIT`, `PM_STARTUP`, `PM_RECOVERY`, `PM_HOT_STANDBY`, `PM_RUN`, `PM_STOP_BACKENDS`, `PM_WAIT_BACKUP`, `PM_WAIT_BACKENDS`, `PM_SHUTDOWN`, `PM_SHUTDOWN_2`, `PM_WAIT_DEAD_END`, `PM_NO_CHILDREN`. |
| **Promote** / **Promotion** | The standby-side action that ends the redo loop, allocates a new TLI, and flips `pg_control` to `DB_IN_PRODUCTION`. Distinct from **failover**. |
| **Promote signal file** | The file `$PGDATA/promote`. When present, `CheckForStandbyTrigger` returns true. |
| **Recovery** | The whole subsystem: the receive-and-replay side of WAL. Includes crash, archive, and standby variants. |
| **Recovery conflict** | A situation where applying a WAL record would invalidate a still-open standby query, lock, or buffer pin. Resolved by the `Resolve*` family in `standby.c`. |
| **`recovery.signal`** | File whose presence requests **archive recovery**. Set by the operator before starting the cluster. |
| **Redo** | The per-record apply step. Accomplished by an rmgr's `rm_redo` callback. |
| **Replay** | Synonym for redo, used informally for whole-WAL actions ("replay this segment"). The codebase prefers "redo" for the per-record step. |
| **Restartpoint** | The recovery analogue of a checkpoint. Flushes buffers, advances `minRecoveryPoint`, recycles `pg_wal/`. Does **not** write a CHECKPOINT WAL record. Misnomer to call it a "standby checkpoint". |
| **Resource manager (rmgr)** | A registered set of redo / desc / decode callbacks for a class of WAL records. 22 built-in (rmid 0..21) plus extension custom rmgrs (rmid 128..255). |
| **`rm_redo`** | The redo callback method on an rmgr. Dispatched from `ApplyWalRecord` via `GetRmgr(rmid).rm_redo`. |
| **`rm_startup` / `rm_cleanup`** | Optional one-shot hooks called by `RmgrStartup` / `RmgrCleanup` around the redo loop. Used by btree, gin, gist, spgist for incomplete-split tracking. |
| **Signal file** | A file in `$PGDATA` whose presence configures recovery. Includes `recovery.signal`, `standby.signal`, and `promote`. Pre-12 terminology used "trigger file" — that name is no longer used in the codebase. |
| **SLRU** | Simple LRU. PostgreSQL's pre-WAL-era page-cached file abstraction. Used for `pg_xact`, `pg_subtrans`, `pg_multixact`, `pg_commit_ts`, etc. |
| **Snapshot conflict horizon** | A `TransactionId` carried in a heap-pruning or btree-deletion record. Indicates the oldest xid that could still need to see the about-to-be-removed data. |
| **Standby** | A cluster running with `standby.signal` present. Continuous recovery + optional hot standby. Configured for streaming via `primary_conninfo`. |
| **Standby checkpoint** | A misnomer for **restartpoint**. Avoid. |
| **`standby.signal`** | File whose presence requests **standby mode** (= archive recovery + continuous + streaming). |
| **Startup process** | The postmaster child that runs the redo loop. Always capitalize. Forked once per recovery; entered via `StartupProcessMain → StartupXLOG`. |
| **Switchpoint** | The LSN at which a timeline forks. Recorded in the new TLI's history file. |
| **Timeline (TLI)** | A version of the WAL stream. Allocated as `parent + 1` at promotion. Recorded in `<TLI>.history`. |
| **VirtualTransactionId (VXID)** | A `(backendId, localXid)` pair that uniquely identifies a backend's current transaction context, even when no XID has been assigned. Used as victim selector for recovery conflicts. |
| **WAL** | Write-ahead log. The append-only sequence of records describing every change to durable state. |
| **Walreceiver** | The auxiliary process on a standby that maintains a libpq connection to the primary, receives WAL bytes, writes to `pg_wal/`, and signals the Startup process. |
| **Walsender** | The auxiliary process on a primary that sends WAL bytes via a libpq connection to a downstream walreceiver. |
| **Warm standby** | A recovering cluster that does **not** accept queries (`hot_standby = off`). Largely obsolete; almost everyone uses hot standby now. |
| **`XLogPrefetcher`** | Wraps `XLogReader` to issue `posix_fadvise(WILLNEED)` ahead of the redo loop's read. Enabled by `recovery_prefetch`. |
| **`XLogReader`** | Source-agnostic WAL reader. Caller plugs in `XLogReaderRoutine` callbacks for `page_read`, `segment_open`, `segment_close`. Shared across recovery, walsender, pg_waldump, pg_rewind. |

## See also

* [appendix_symbol_index.md](appendix_symbol_index.md) for symbol-name → file:line lookup.
* [appendix_data_structures.md](appendix_data_structures.md) for `XLogRecoveryCtl`, `WalRcvData`, etc.
