# Appendix — Glossary

[Up: index.md](index.md)  |  [Prev: appendix_symbol_index.md](appendix_symbol_index.md)  |  [Next: appendix_data_structures.md](appendix_data_structures.md)

This glossary defines every metadata-specific term used in this
document. Where a synonym exists, the *preferred PostgreSQL
implementation term* is given first; the synonym is listed in
parentheses with a note about why it is avoided.

---

**Async commit**. A commit mode (`synchronous_commit = off`) where
the transaction returns success before its WAL has been flushed. CLOG
must use the per-page `group_lsn` array to ensure that no CLOG bit is
written ahead of the corresponding WAL flush. See [09 CLOG](09_clog.md).

**Bank lock**. The LWLock guarding a contiguous group of slots in an
SLRU. Computed as `bank_locks[pageno % nbanks]`. Replaces the older
single global SLRU lock. See [08 SLRU Framework](08_slru_framework.md).

**BKI** ("backend interface"). The bootstrap language emitted by
genbki.pl into `postgres.bki` and consumed by `postgres --boot`. See
[03 Catalog Data Model](03_catalog_data_model_and_bootstrap.md).

**Bootstrap circularity**. The catch-22 that the relcache cannot read
pg_class to find pg_class's relfilenode. Solved by the relmapper. See
[07 Relmapper](07_relmapper.md), [21 §1](21_deep_dives.md).

**Catalog**. A table in the `pg_catalog` schema holding cluster-level
metadata. (*Avoid*: "data dictionary" — that term implies
ANSI/ORDBMS-style read-only metadata, while pg_catalog tables are
ordinary heap relations subject to MVCC.)

**Catcache** (catalog cache). The lowest-level catalog row cache, one
hash table per `(catalog, index, key columns)` triple. See
[05 Catalog Caches](05_catalog_caches.md).

**Cache invalidation**. The mechanism by which one backend tells all
other backends that its catalog changes have committed and they must
flush their caches. See [06 Cache Invalidation](06_cache_invalidation.md).

**catversion**. The build-time `CATALOG_VERSION_NO` integer that
identifies the catalog header layout. Mismatch with `pg_control` is a
fatal error. See [03](03_catalog_data_model_and_bootstrap.md), [21 §16](21_deep_dives.md).

**CCI** (CommandCounterIncrement). The point at which catalog
mutations made earlier in the transaction become visible to later
commands of the same transaction. Triggers
`CommandEndInvalidationMessages`.

**Checkpoint**. A point in WAL where every dirty buffer has been
flushed to disk and `pg_control` has been updated with cluster-wide
cursors. After a checkpoint, recovery can start from the redo
pointer. (*Avoid*: "sync point" — that term is ambiguous with
`fsync` semantics.)

**CLOG** (commit log). The per-XID 2-bit commit/abort status SLRU at
`$PGDATA/pg_xact/`. (*Avoid*: "commit log file" — `pg_xact` is a
directory of segments, not one file.) See [09 CLOG](09_clog.md).

**CommitTs**. The per-XID commit-timestamp + RepOriginId SLRU at
`$PGDATA/pg_commit_ts/`. Active only when `track_commit_timestamp =
on`. See [11 Commit Timestamps](11_commit_timestamps.md).

**Custom rmgr**. An extension-registered WAL resource manager, using
the reserved `RM_EXPERIMENTAL_ID..RM_MAX_ID` range. See
[17 Hooks and Extensibility](17_hooks_and_extensibility.md), [21 §18](21_deep_dives.md).

**Dependency** (pg_depend). A row that records "this object depends
on that object". The CASCADE semantics of `DROP` are computed by
walking pg_depend.

**Fork**. A relation has separate storage forks: `MAIN_FORKNUM` for
the heap, `FSM_FORKNUM` for the free-space map, `VM_FORKNUM` for the
visibility map, `INIT_FORKNUM` for unlogged-relation initialization
data.

**FPI** (full-page image). A complete copy of an 8 KiB page embedded
in a WAL record, used to recover from torn-page hazards. (*Note*:
the GUC name is `full_page_writes`; "FPI" is the in-source
abbreviation.)

**FPW** (full-page writes). The GUC enabling FPI emission for the
first dirty buffer write after every checkpoint. Defaults to `on`.

**Frozen**. A heap tuple is frozen when its `xmin` (and `xmax` if
relevant) are old enough that vacuum can confidently mark them as
"infinitely old"; visibility checks for frozen tuples skip CLOG.

**FSM** (Free Space Map). The per-heap-page free-space hint stored
in `FSM_FORKNUM`. Hint-only; vacuum repairs. See
[14 Free Space Map](14_free_space_map.md).

**Group commit (CLOG)**. The leader-follower scheme in
`TransactionGroupUpdateXidStatus` that batches concurrent CLOG
updates on the same page under one bank-lock acquisition. See
[09 CLOG](09_clog.md), [21 §7](21_deep_dives.md).

**Hint bit**. A flag on a heap tuple's `t_infomask` that records
visibility info recomputable from CLOG (e.g., `HEAP_XMIN_COMMITTED`).
Setting a hint does not normally need WAL traffic. (*Avoid*:
"shortcut bit" — non-standard.) See [21 §14](21_deep_dives.md).

**Inval (inval.c)**. The per-transaction outbox where catalog
mutations queue invalidation messages. See
[06 Cache Invalidation](06_cache_invalidation.md).

**LSN** (log sequence number). A 64-bit position in the WAL stream.
PostgreSQL's WAL-before-data rule says no buffer may be flushed to
disk until WAL up to its LSN has been flushed.

**Mapped catalog**. A catalog whose relfilenode is recorded in
`pg_filenode.map` rather than `pg_class.relfilenode`. The 4 nailed
local catalogs and all 11 shared catalogs are mapped. See
[07 Relmapper](07_relmapper.md).

**MultiXact** (multi-XID). A small integer representing a *set* of
transactions that lock or update the same row. Stored as two SLRUs:
`pg_multixact/offsets` (offset-per-multi) and `pg_multixact/members`
(packed array of TransactionId+status). See
[12 MultiXact](12_multixact.md).

**Nailed catalog**. A catalog whose relcache descriptor is hard-coded
via `formrdesc` (because reading pg_class to build it is a
chicken-and-egg). The four nailed catalogs are `pg_class`,
`pg_attribute`, `pg_proc`, `pg_type`. See
[03](03_catalog_data_model_and_bootstrap.md), [05](05_catalog_caches.md).

**Negative entry**. A `CatCTup` with `negative = true` recording "no
matching row" so subsequent identical lookups can short-circuit.
Invalidated by hash-bucket purges. See [21 §3](21_deep_dives.md).

**Page-state machine**. The four-state transition diagram for an
SLRU slot: `EMPTY → READING → VALID → WRITING`. See
[08 SLRU Framework](08_slru_framework.md).

**pg_control**. The 512-byte cluster-anchor file at
`$PGDATA/global/pg_control` carrying `system_identifier`,
`pg_control_version`, `catalog_version_no`, latest checkpoint, and
metadata cursors. See [03 §pg_control](03_catalog_data_model_and_bootstrap.md), [21 §17](21_deep_dives.md).

**pg_filenode.map**. The relmapper's persistent file. See
[07 Relmapper](07_relmapper.md).

**pg_internal.init**. The relcache shortcut file: a serialized
snapshot of nailed/built relcache entries, used to skip the slow
catalog-scan path at backend start. See
[05](05_catalog_caches.md), [21 §2](21_deep_dives.md).

**Pin (buffer pin)**. A non-blocking refcount on a buffer, preventing
the buffer-manager from evicting it. Distinct from the buffer's lock.
The pin-VM-before-lock-heap protocol uses pin's non-blocking
property to avoid deadlock. See [21 §13](21_deep_dives.md).

**Recovery**. The process at backend startup that reads pg_control,
sets cursors, and replays WAL from the redo pointer to the end. Run
by `StartupXLOG`. See [16 Checkpoints and Recovery](16_checkpoints_and_recovery.md).

**Redo**. The act of replaying a WAL record. The function that
implements redo for one rmgr is named `<module>_redo`. (*Note*:
"WAL replay" is also acceptable, particularly when describing the
standby path.)

**Relcache**. The per-relation cache of `RelationData` structs.
Top of the catalog cache stack. See [05 Catalog Caches](05_catalog_caches.md).

**Relmap, relmapper**. The mechanism for mapping a catalog OID to a
relfilenode for nailed and shared catalogs. (*Avoid*: "filenode
map" — non-standard term.) See [07 Relmapper](07_relmapper.md).

**Restartpoint**. A standby-side analog of a checkpoint, created at
each replayed `XLOG_CHECKPOINT_ONLINE` record. See [16](16_checkpoints_and_recovery.md).

**Resource manager (rmgr)**. A dispatch entry in `rmgrlist.h` that
binds an `RmgrId` to a redo function and a desc function. See
[15](15_persistence_and_wal_records.md), [21 §18](21_deep_dives.md).

**Shared catalog**. A catalog with one physical file across all
databases (e.g., `pg_authid`). Lives in `pg_global` tablespace.
There are eleven shared catalogs. See [03](03_catalog_data_model_and_bootstrap.md).

**SI_RESET**. The sinval-overflow path: a backend that has fallen
too far behind has its `resetState` flag set, and on its next
`AcceptInvalidationMessages` it wipes every catcache and relcache
entry. Over-invalidation is safe; under-invalidation is not. See
[06](06_cache_invalidation.md), [21 §4](21_deep_dives.md).

**Sinval** (shared invalidation). The shared-memory ring buffer that
distributes catalog invalidation messages between backends. See
[06 Cache Invalidation](06_cache_invalidation.md).

**SLRU** (Simple LRU). The bank-locked, page-pool-based on-disk cache
used by CLOG, MultiXact, CommitTs, SUBTRANS, Notify, Serial.
(*Avoid*: "circular log" — SLRU is not strictly circular.) See
[08 SLRU Framework](08_slru_framework.md).

**SUBTRANS**. The per-XID subtransaction-parent SLRU. The only SLRU
that is not WAL-logged. See [10 SUBTRANS](10_subtrans.md), [21 §8](21_deep_dives.md).

**Syscache** (system cache). The middle layer of the catalog cache
stack, indexed by `SysCacheIdentifier`. Wraps catcache. See
[05 Catalog Caches](05_catalog_caches.md).

**System catalog**. Synonym for "catalog". (*Preferred over* "data
dictionary".)

**System index**. An index on a system catalog, declared via
`DECLARE_*INDEX*` in the catalog header.

**Tier 1 / Tier 2**. Importance annotations on individual symbols,
derived from the auto-extracted ranking in stage 1. Tier 1 = score ≥
0.85; these symbols get full deep-dive treatment.

**Toast**. The mechanism for storing oversized variable-length values
in a sidecar relation. The toast relation is created by
`NewHeapCreateToastTable`. See [04 Catalog Modification APIs](04_catalog_modification_apis.md).

**Visibility map (VM)**. The per-heap-page `ALL_VISIBLE` /
`ALL_FROZEN` bits stored in `VM_FORKNUM`. See
[13 Visibility Map](13_visibility_map.md).

**WAL** (write-ahead log). PostgreSQL's redo log. Every persistent
metadata change is described by some WAL record. See chapter
[15 Persistence and WAL Records](15_persistence_and_wal_records.md).

**Wraparound**. The condition where the 32-bit XID counter (or
MultiXactId counter, or member-offset counter) is about to overflow.
Vacuum is responsible for advancing the corresponding `oldest*`
cursor in `pg_control` so the wraparound stays a safe distance ahead.
See [09 CLOG](09_clog.md), [12 MultiXact](12_multixact.md), [21 §15](21_deep_dives.md).

**XID** (transaction ID). A 32-bit identifier assigned by
`GetNewTransactionId`. The 64-bit `FullTransactionId` form carries an
epoch counter to disambiguate post-wraparound XIDs.

**Zero page (XLOG_*_ZEROPAGE)**. A WAL record that creates a fresh
SLRU page. Emitted before any commit-bit / member / timestamp record
that would write into it. See [08](08_slru_framework.md), [09](09_clog.md).

---

[Up: index.md](index.md)  |  [Prev: appendix_symbol_index.md](appendix_symbol_index.md)  |  [Next: appendix_data_structures.md](appendix_data_structures.md)
