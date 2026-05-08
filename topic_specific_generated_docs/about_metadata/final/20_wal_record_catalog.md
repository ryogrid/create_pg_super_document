# 20 — Metadata WAL Record Catalog

[Up: index.md](index.md)  |  [Prev: 19 SLRU Users Catalog](19_slru_users_catalog.md)  |  [Next: 21 Deep Dives](21_deep_dives.md)

## Prerequisites

- [15 Persistence and WAL Records](15_persistence_and_wal_records.md) — the rmgrlist dispatch.
- [16 Checkpoints and Recovery](16_checkpoints_and_recovery.md) — when redo runs.

This chapter is a per-record reference for every metadata-affecting
WAL record. Records are grouped by their resource manager (rmgr).

Each entry follows a standardized template:

- **rmgr ID** and the redo function it dispatches to.
- **Info byte** (the secondary tag within an rmgr).
- **Payload struct** with field-level annotations.
- **Emitter**: the C function that calls `XLogInsert`.
- **Redo function**: the C function called during WAL replay.
- **Redo path**: which SLRU page or buffer-manager block is mutated.
- **What it makes durable**.
- **Full-page image policy**.
- **Standby implications**.

Total: 30 WAL record types across 9 rmgrs. Quick reference in
[appendix_wal_record_quick_reference.md](appendix_wal_record_quick_reference.md).

## Section RM_XACT_ID — Transaction records

`RM_XACT_ID = "Transaction"`, redo: `xact_redo`
(`src/backend/access/transam/xact.c`) which dispatches to
`xact_redo_commit` / `xact_redo_abort` based on the info byte.

These records are formally part of the XACT rmgr, not the metadata
rmgrs, but every commit and abort updates CLOG, CommitTs (when
enabled), and broadcasts cache invalidations — so they are central
to metadata persistence. Detailed walkthrough is in chapter
[15 Persistence and WAL Records](15_persistence_and_wal_records.md).

### XLOG_XACT_COMMIT  (info 0x00)

- **Header**: `xact.h`.
- **Payload**: `xl_xact_commit` plus optional sub-records gated by
  the `xinfo` flag bits (`xl_xact_xinfo`, `xl_xact_dbinfo`,
  `xl_xact_subxacts`, `xl_xact_relfilelocators`, `xl_xact_invals`,
  `xl_xact_twophase`, `xl_xact_origin`).
- **Emitter**: `RecordTransactionCommit` (`xact.c:1304`).
- **Redo**: `xact_redo_commit` (`xact.c:6068`):
  1. `TransactionIdCommitTree(xid, parsed->nsubxacts, parsed->subxacts)` —
     update CLOG bits.
  2. `ProcessCommittedInvalidationMessages(parsed->msgs, ...)` —
     broadcast sinval messages.
  3. `TransactionTreeSetCommitTsData(...)` — update CommitTs.
  4. `smgrDoPendingDeletes(true)` — execute file unlinks.
- **Makes durable**: the entire metadata effect of a top-level
  transaction (CLOG, dropped relfilenodes, sinval, commit timestamp).
- **Standby effects**: full cache invalidation broadcast.

### XLOG_XACT_PREPARE  (info 0x10)

- **Header**: `xact.h`.
- **Payload**: `TwoPhaseFileHeader` + sub-XIDs + locks + invalidation
  messages + relations.
- **Emitter**: 2PC machinery (`twophase.c::EndPrepare`).
- **Redo**: `xact_redo` reconstructs the 2PC state file in
  `pg_twophase/<gid>` so a subsequent COMMIT_PREPARED or
  ABORT_PREPARED can finalize.
- **Makes durable**: a 2PC PREPARE; the corresponding COMMIT or
  ABORT comes later.

### XLOG_XACT_ABORT  (info 0x20)

- **Header**: `xact.h`.
- **Payload**: `xl_xact_abort` (sub-XIDs + dropped relfilelocators).
- **Emitter**: `RecordTransactionAbort` (`xact.c:1723`).
- **Redo**: `xact_redo_abort` (`xact.c:6222`):
  1. `TransactionIdAbortTree(xid, parsed->nsubxacts, parsed->subxacts)`.
  2. `smgrDoPendingDeletes(false)` — undo file creates queued in
     this xact.
- **Makes durable**: aborted XID tree in CLOG plus undo of file
  creates.

### XLOG_XACT_COMMIT_PREPARED  (info 0x30)

- **Payload**: `xl_xact_commit_prepared` (= `xl_xact_commit` +
  prepared-xid).
- **Emitter**: `FinishPreparedTransaction` (commit half).
- **Redo**: same as XLOG_XACT_COMMIT plus removal of the
  `pg_twophase/<gid>` state file.

### XLOG_XACT_ABORT_PREPARED  (info 0x40)

- **Payload**: `xl_xact_abort_prepared`.
- **Emitter**: `FinishPreparedTransaction` (abort half).
- **Redo**: same as XLOG_XACT_ABORT plus removal of 2PC state file.

### XLOG_XACT_ASSIGNMENT  (info 0x50)

- **Payload**: `xl_xact_assignment` (parent XID + sub-XID array).
- **Emitter**: `xact.c` — when a backend's in-shmem subxid cache
  overflows, it logs the parent → sub-XID assignment so standby
  snapshot building can correctly attribute sub-XIDs.
- **Redo**: bumps `KnownAssignedXids` on hot standby; otherwise
  no-op.
- **Makes durable**: the top-XID assignment for a list of sub-XIDs.

### XLOG_XACT_INVALIDATIONS  (info 0x60)

- **Payload**: `SharedInvalidationMessage[]`.
- **Emitter**: mid-transaction inval emit, used by logical decoding.
- **Redo**: replays the invalidations on the standby (or in the
  decoding output stream) so DDL effects are correctly seen by
  logical-decoding consumers.
- **Makes durable**: cache invalidations emitted *during* a
  transaction (rather than at commit) so they survive in WAL.

## Section RM_XLOG_ID — checkpoints, NEXTOID, FPI

`RM_XLOG_ID = "XLOG"`, redo: `xlog_redo`
(`src/backend/access/transam/xlog.c`).

This rmgr handles the cluster-wide control records.

### XLOG_CHECKPOINT_SHUTDOWN  (info 0x00)

- **Header**: `pg_control.h:68`.
- **Payload**: `CheckPoint` struct (`pg_control.h:35`).
  ```c
  typedef struct CheckPoint
  {
      XLogRecPtr        redo;
      TimeLineID        ThisTimeLineID;
      TimeLineID        PrevTimeLineID;
      bool              fullPageWrites;
      int               wal_level;
      FullTransactionId nextXid;
      Oid               nextOid;
      MultiXactId       nextMulti;
      MultiXactOffset   nextMultiOffset;
      TransactionId     oldestXid;
      Oid               oldestXidDB;
      MultiXactId       oldestMulti;
      Oid               oldestMultiDB;
      pg_time_t         time;
      TransactionId     oldestCommitTsXid;
      TransactionId     newestCommitTsXid;
      TransactionId     oldestActiveXid;
  } CheckPoint;
  ```
- **Emitter**: `CreateCheckPoint(CHECKPOINT_IS_SHUTDOWN | ...)`.
- **Redo**: `xlog_redo` updates `ShmemVariableCache` cursors
  (nextXid, nextOid, nextMulti, ..., oldestCommitTsXid). After this record
  is processed, the standby's WAL replay can be considered "consistent" if
  there were no pending in-flight transactions at shutdown.
- **Makes durable**: a complete snapshot of cluster metadata cursors.

### XLOG_CHECKPOINT_ONLINE  (info 0x10)

- **Header**: `pg_control.h:69`.
- **Payload**: same `CheckPoint` struct, but with `oldestActiveXid` set
  (used to initialize hot-standby snapshot building).
- **Emitter**: `CreateCheckPoint` (without IS_SHUTDOWN flag) — the
  routine periodic checkpoint.
- **Redo**: same as SHUTDOWN, but the standby uses `oldestActiveXid` to
  prime its KnownAssignedXids state for hot standby.

### XLOG_NEXTOID  (info 0x30)

- **Header**: `pg_control.h:71`.
- **Payload**: `Oid nextOidValue` (4 bytes).
- **Emitter**: `GetNewObjectId()` — every `VAR_OID_PREFETCH` (currently 8192)
  OIDs allocated, the next "checkpoint" OID is logged so a crash never
  hands out the same OID twice.
- **Redo**: `xlog_redo` advances `ShmemVariableCache->nextOid` to at least
  the logged value.
- **Makes durable**: the OID counter advance.

### XLOG_FPI  (info 0xB0)

- **Header**: `pg_control.h:79`.
- **Payload**: empty (the data is in the registered buffer's full-page image).
- **Emitter**: ad-hoc — used by VACUUM page-prune in some cases, and other
  routines that need to inject an FPI for non-WAL-logged page changes.
- **Redo**: `xlog_redo` restores the FPI to the page.
- **Makes durable**: a torn-page-safe initial image for a page about to be
  modified by a non-WAL-logged change.

### XLOG_FPI_FOR_HINT  (info 0xA0)

- **Header**: `pg_control.h:78`.
- **Payload**: empty (FPI in the registered buffer).
- **Emitter**: `MarkBufferDirtyHint` when checksums or `wal_log_hints` is
  enabled. Triggered by hint-bit-only changes (e.g., setting
  `HEAP_XMIN_COMMITTED`).
- **Redo**: same as XLOG_FPI — restore the page from FPI.
- **Makes durable**: the page contents at the moment of the hint-bit
  write, so a torn write does not corrupt non-hint data.

### XLOG_CHECKPOINT_REDO  (info 0xE0)

- **Header**: `pg_control.h:82`.
- **Payload**: small marker (no useful data — the LSN of the record itself
  is the data).
- **Emitter**: `CreateCheckPoint` immediately after computing the redo
  pointer. Inserted at the precise WAL location that becomes the recovery
  start point.
- **Redo**: no-op.
- **Makes durable**: the precise LSN at which redo will start. Important
  because the actual XLOG_CHECKPOINT_* record can be far later (the
  checkpoint flushes all dirty buffers, which can take minutes).

### XLOG_NOOP  (info 0x20)

- **Payload**: variable padding bytes.
- **Emitter**: `XLogInsert(XLOG_NOOP)` for pg_walfile_name boundary alignment.
- **Redo**: no-op.

### XLOG_SWITCH  (info 0x40)

Forces switching to a new WAL segment. Used by base backup.

### XLOG_BACKUP_END  (info 0x50)

Marks the end of a backup; used by `pg_backup_stop`.

### XLOG_PARAMETER_CHANGE  (info 0x60)

- **Payload**: changes to wal-level-relevant GUCs.
- **Emitter**: when ALTER SYSTEM SET wal_level (or a few others) takes effect.
- **Redo**: standby errors out if the parameter change reduces wal_level
  below what the standby needs (e.g., changing from `replica` to `minimal`).

### XLOG_RESTORE_POINT  (info 0x70)

User-named restore point for PITR.

### XLOG_FPW_CHANGE  (info 0x80)

Tracks `full_page_writes` toggling.

### XLOG_END_OF_RECOVERY  (info 0x90)

Inserted at the end of crash recovery.

### XLOG_OVERWRITE_CONTRECORD  (info 0xD0)

Used to overwrite a continuation-record header when an aborted WAL
record was partially written.

### Cross-references

- `[16 Checkpoints and Recovery](16_checkpoints_and_recovery.md)` — checkpoint creation flow.
- `[15 Persistence and WAL Records](15_persistence_and_wal_records.md)` — XLOG_FPI_FOR_HINT context.


## Section RM_CLOG_ID

`RM_CLOG_ID = "CLOG"`, redo function: `clog_redo`
(`src/backend/access/transam/clog.c:1107`).

### XLOG_CLOG_ZEROPAGE  (info 0x00)

- **Header**: `clog.h:55`.
- **Payload**: `int64 pageno` (8 bytes).
- **Emitter**: `ExtendCLOG()` (`clog.c`) — when `GetNewTransactionId`
  advances the next XID onto a new CLOG page.
- **Redo**: `clog_redo` calls `SimpleLruZeroPage(XactCtl, pageno)` then
  `SimpleLruWritePage(XactCtl, slot)` so the standby has the freshly-zeroed
  page on disk before any commit-bit update record references it.
- **Makes durable**: existence of a fresh CLOG page covering 32768 XIDs
  (`CLOG_XACTS_PER_PAGE`).
- **Full-page image**: not applicable (no data page beyond the zeroed
  initial state).
- **Standby effects**: a zero page on disk; no cache invalidations.

### XLOG_CLOG_TRUNCATE  (info 0x10)

- **Header**: `clog.h:56`.
- **Payload**:
  ```c
  /* clog.h:32 */
  typedef struct xl_clog_truncate
  {
      int64         pageno;
      TransactionId oldestXact;
      Oid           oldestXactDb;
  } xl_clog_truncate;
  ```
- **Emitter**: `TruncateCLOG()` (`clog.c`) — called from `vac_truncate_clog`
  after vacuum advances `ShmemVariableCache->oldestClogXid`.
- **Redo**: `clog_redo`:
  1. `AdvanceOldestClogXid(xlrec.oldestXact)` — updates the standby's
     `oldestClogXid`.
  2. `SimpleLruTruncate(XactCtl, xlrec.pageno)` — drops segment files
     before pageno.
- **Makes durable**: the cluster-wide CLOG truncation cutoff.
- **Full-page image**: none.
- **Standby effects**: shrinks pg_xact directory; `oldestXid` cursor
  advances. No cache invalidations.

### Why no XLOG_CLOG_SETSTATUS?

Setting the commit-bit for an XID does not need a CLOG-specific WAL record.
The corresponding `XLOG_XACT_COMMIT` (or `XLOG_XACT_ABORT`) is the durable
truth: its redo function (`xact_redo_commit` / `_abort`) calls
`TransactionIdCommitTree` / `TransactionIdAbortTree` which writes the CLOG
bit. So commit-bit setting is *implicit* in the XACT WAL stream.

The only standalone CLOG WAL records are zero-page (which is needed because
SimpleLruZeroPage is the gateway to a non-existent page) and truncate
(which is a side-effect of vacuum's freeze-horizon advance).

### Cross-references

- `[09 CLOG](09_clog.md)` — full CLOG design.
- `[19 SLRU Users Catalog § CLOG](19_slru_users_catalog.md)` — pg_xact directory layout.
- `[15 Persistence and WAL Records](15_persistence_and_wal_records.md)` — XACT records that
  implicitly drive CLOG.


## Section RM_MULTIXACT_ID

`RM_MULTIXACT_ID = "MultiXact"`, redo: `multixact_redo`
(`src/backend/access/transam/multixact.c`).

### XLOG_MULTIXACT_ZERO_OFF_PAGE  (info 0x00)

- **Header**: `multixact.h:68`.
- **Payload**: `int64 pageno` (8 bytes).
- **Emitter**: `GetNewMultiXactId()` when the new multi falls on a fresh
  offsets page.
- **Redo**: `multixact_redo` calls `SimpleLruZeroPage(MultiXactOffsetCtl,
  pageno)` then `SimpleLruWritePage`.
- **Makes durable**: zeroing of a fresh `pg_multixact/offsets` page.
- **Standby effects**: new page on disk.

### XLOG_MULTIXACT_ZERO_MEM_PAGE  (info 0x10)

- **Header**: `multixact.h:69`.
- **Payload**: `int64 pageno`.
- **Emitter**: `GetNewMultiXactId()` when the new offset falls on a fresh
  members page.
- **Redo**: `SimpleLruZeroPage(MultiXactMemberCtl, pageno)`.
- **Makes durable**: zeroing of a fresh `pg_multixact/members` page.

### XLOG_MULTIXACT_CREATE_ID  (info 0x20)

- **Header**: `multixact.h:70`.
- **Payload**:
  ```c
  typedef struct xl_multixact_create
  {
      MultiXactId        mid;
      MultiXactOffset    moff;
      int32              nmembers;
      MultiXactMember    members[FLEXIBLE_ARRAY_MEMBER];
  } xl_multixact_create;
  ```
- **Emitter**: `MultiXactIdCreateFromMembers` → `RecordNewMultiXact`. The
  WAL record is written *before* the SLRU pages are updated.
- **Redo**: `multixact_redo` → `RecordNewMultiXact(mid, moff, nmembers,
  members)` → updates both the offsets SLRU (with `moff` at slot
  `mid`) and the members SLRU (with the member array starting at offset
  `moff`). Also advances `nextMulti` and `nextMultiOffset` cursors in
  shared memory.
- **Makes durable**: creation of one MultiXactId, including its members.
- **Full-page image**: no (the SLRU pages are written via SimpleLruWritePage
  separately; the WAL record carries enough info to recreate them).

### XLOG_MULTIXACT_TRUNCATE_ID  (info 0x30)

- **Header**: `multixact.h:71`.
- **Payload**:
  ```c
  typedef struct xl_multixact_truncate
  {
      Oid              oldestMultiDB;
      MultiXactId      startTruncOff;
      MultiXactId      endTruncOff;
      MultiXactOffset  startTruncMemb;
      MultiXactOffset  endTruncMemb;
  } xl_multixact_truncate;
  ```
- **Emitter**: `TruncateMultiXact()` from `vac_truncate_clog`.
- **Redo**:
  1. Update `oldestMulti` and `oldestMultiDB`.
  2. `SimpleLruTruncate(MultiXactOffsetCtl, ...)`.
  3. `SimpleLruTruncate(MultiXactMemberCtl, ...)`.
- **Makes durable**: simultaneous truncation of both offsets and members
  SLRUs.
- **Standby effects**: shrinks pg_multixact/offsets and members directories.

### Cross-references

- `[12 MultiXact](12_multixact.md)` — full design.
- `[19 SLRU Users Catalog § MultiXact Offsets](19_slru_users_catalog.md)`, `multixact_members.md`.


## Section RM_RELMAP_ID

`RM_RELMAP_ID = "RelMap"`, redo: `relmap_redo`
(`src/backend/utils/cache/relmapper.c`).

### XLOG_RELMAP_UPDATE  (info 0x00)

- **Header**: `relmapper.h:25`.
- **Payload**:
  ```c
  /* relmapper.h:27 */
  typedef struct xl_relmap_update
  {
      Oid    dbid;            /* database ID; 0 if shared map */
      Oid    tsid;            /* tablespace OID; pg_global if shared */
      int32  nbytes;          /* size of the embedded RelMapFile */
      char   data[FLEXIBLE_ARRAY_MEMBER];
  } xl_relmap_update;
  ```
  The `data` payload is a complete `RelMapFile` (524 bytes typical):
  magic + count + 64 RelMapping entries + crc.

- **Emitter**: `perform_relmap_update()` (called from
  `AtEOXact_RelationMap(true)` or `RelationMapFinishBootstrap`). Always
  emitted *before* the on-disk rename, so a standby always sees the new
  map even if the primary crashes mid-rename.

- **Redo**: `relmap_redo`:
  1. Validate the embedded RelMapFile (magic + crc).
  2. `write_relmap_file_internal(buffer, dbid, tsid)` — same atomic
     temp+fsync+rename+fsync-parent-dir as the primary.
  3. Update in-memory `shared_map` or `local_map`.
  4. `CacheInvalidateRelmap` so other backends re-read.

- **Makes durable**: a complete new RelMapFile contents (catalog OID →
  relfilenode mapping for nailed/shared catalogs).

- **Full-page image**: not applicable — the entire RelMapFile (≤524 bytes)
  is in the WAL record itself.

- **Standby effects**:
  - File system: `global/pg_filenode.map` or
    `base/<dbid>/pg_filenode.map` is rewritten.
  - In-memory: `shared_map` or `local_map` updated.
  - Caches: every backend on the standby gets a `SHAREDINVALRELMAP_ID`
    sinval message; `RelationMapInvalidate` re-reads the file. Relcache
    entries for the affected catalog have `rd_node` updated on next
    open.

### When is XLOG_RELMAP_UPDATE emitted?

- VACUUM FULL on a mapped catalog: the new file is built, `relfilenode`
  is updated in the relmap, the WAL record carries the new map.
- CLUSTER on a mapped catalog: same.
- REINDEX on a mapped catalog's index: same (the index itself is mapped).
- `RelationMapFinishBootstrap` at initdb time.
- TRUNCATE on a mapped catalog: NOT allowed; tablecmds.c rejects it.

### Why the full file?

The 524-byte file is small; it is cheaper to log the whole file than to log
diffs and risk inconsistent state. Each WAL record is self-describing:
replaying any one XLOG_RELMAP_UPDATE results in a fully-valid map.

### Cross-references

- `[07 Relmapper](07_relmapper.md)` — full design and atomic-write protocol.
- `[03 Catalog Data Model](03_catalog_data_model_and_bootstrap.md)` — nailed/shared catalogs
  that depend on the relmap.


## Section RM_SMGR_ID — storage manager

`RM_SMGR_ID = "Storage"`, redo: `smgr_redo`
(`src/backend/catalog/storage.c:965`).

### XLOG_SMGR_CREATE  (info 0x10)

- **Header**: `storage_xlog.h:30`.
- **Payload**:
  ```c
  typedef struct xl_smgr_create
  {
      RelFileLocator rlocator;     /* (spcOid, dbOid, relNumber) */
      ForkNumber     forkNum;      /* MAIN_FORKNUM, INIT_FORKNUM, etc. */
  } xl_smgr_create;
  ```

- **Emitter**: `log_smgrcreate()` (storage.c:186), called from
  `RelationCreateStorage` for permanent relations (skipped for unlogged
  and temp).

- **Redo**: `smgr_redo`:
  1. `smgropen(xlrec->rlocator, INVALID_PROC_NUMBER)`.
  2. `smgrcreate(srel, xlrec->forkNum, true /* is_redo */)` — actually
     creates the file. `is_redo = true` makes smgrcreate tolerant of the
     file already existing.
  3. If forkNum == MAIN_FORKNUM, the FSM and VM forks may also need to be
     ready for subsequent records.

- **Makes durable**: physical creation of one fork of one relfilenode.

- **Full-page image**: not applicable — smgrcreate produces a 0-byte file.

- **Standby effects**: a new file appears in `base/<dbid>/<relfilenode>`
  (or with the fork suffix `_fsm`, `_vm`, `_init`).

- **XLR_SPECIAL_REL_UPDATE**: the record carries this flag, indicating it
  affects relation storage outside of normal heap pages — used by
  pg_rewind / pg_basebackup to know which files to track.

### XLOG_SMGR_TRUNCATE  (info 0x20)

- **Header**: `storage_xlog.h:31`.
- **Payload**:
  ```c
  typedef struct xl_smgr_truncate
  {
      BlockNumber    blkno;       /* heap fork's new block count */
      RelFileLocator rlocator;
      uint32         flags;        /* SMGR_TRUNCATE_HEAP / FSM / VM / ALL */
  } xl_smgr_truncate;
  ```

- **Emitter**: `RelationTruncate()` (storage.c) — invoked by TRUNCATE,
  by VACUUM when shrinking a relation, and by ALTER SEQUENCE.

- **Redo**: `smgr_redo`:
  1. `smgrtruncate(srel, MAIN_FORKNUM, blkno)` if SMGR_TRUNCATE_HEAP.
  2. `visibilitymap_prepare_truncate(rel, blkno)` if SMGR_TRUNCATE_VM —
     truncates the VM fork.
  3. `FreeSpaceMapPrepareTruncateRel(rel, blkno)` if SMGR_TRUNCATE_FSM —
     truncates the FSM fork.

- **Makes durable**: physical truncation of a relation's main fork plus
  in-lockstep truncation of its VM and FSM forks.

- **Full-page image**: not applicable.

- **Standby effects**: relation files shrink; VM and FSM forks shrink
  proportionally.

### Pending-delete bookkeeping

The storage.c module also maintains an in-memory `pendingDeletes` list,
not directly WAL-logged. Entries are added by `RelationCreateStorage`
(`atCommit = false` — undo create on abort) and `RelationDropStorage`
(`atCommit = true` — execute unlink at commit).

The list is consulted by:
- `smgrDoPendingDeletes(true)` at commit — unlinks files marked atCommit.
- `smgrDoPendingDeletes(false)` at abort — unlinks files marked
  !atCommit (the ones we just created).
- `xact_redo_commit` — replays from `xl_xact_relfilelocators` in
  `xl_xact_commit`.

This is why XLOG_XACT_COMMIT carries a list of dropped relfilelocators:
the standby needs to know which files to unlink at commit replay.

### Cross-references

- `[04 Catalog Modification APIs](04_catalog_modification_apis.md)` — RelationCreateStorage,
  RelationDropStorage, smgrDoPendingDeletes.
- `[15 Persistence and WAL Records](15_persistence_and_wal_records.md)` — XACT records carry
  the pending-delete list.


## Section RM_HEAP2_ID — visibility map (XLOG_HEAP2_VISIBLE)

`RM_HEAP2_ID = "Heap2"`, redo: `heap2_redo` → `heap_xlog_visible`
(`src/backend/access/heap/heapam.c`).

### XLOG_HEAP2_VISIBLE  (info 0x40)

- **Header**: `heapam_xlog.h:62`.
- **Payload**:
  ```c
  typedef struct xl_heap_visible
  {
      TransactionId cutoff_xid;
      uint8         flags;          /* VISIBILITYMAP_ALL_VISIBLE / ALL_FROZEN */
  } xl_heap_visible;
  ```

  Two registered buffers:
  - **block 0**: the heap page (for setting `PD_ALL_VISIBLE`).
  - **block 1**: the VM page (for setting the bit).

- **Emitter**: `lazy_scan_heap` (`vacuumlazy.c`) when vacuum determines a
  heap page is now all-visible / all-frozen and calls `visibilitymap_set`.

- **Redo**: `heap_xlog_visible`:
  1. If the heap buffer registration is present and the page was
     full-page-image-included, restore the heap page (which sets
     `PD_ALL_VISIBLE`).
  2. If the heap buffer is *not* full-page-image, set
     `PageHeader::pd_flags |= PD_ALL_VISIBLE` directly.
  3. If the VM buffer registration is present:
     - If full-page-image: restore the VM page.
     - Else: open the VM page, set `byte[mapByte] |= (flags << mapOffset)`,
       set page LSN.

- **Makes durable**: simultaneous "this heap page is all-visible" hint
  on both the heap-page header and the VM page.

- **Full-page image**: conditional. The VM page's FPI is included when:
  - The VM page's last LSN < the most recent checkpoint's redo pointer
    (the standard torn-page protection).
  - Or `wal_log_hints` / data checksums are enabled.
  In high-throughput workloads, the VM page receives an FPI roughly once
  per checkpoint cycle; subsequent visibilitymap_set calls in the same
  cycle log only the bit changes.

- **Standby effects**: heap page's `PD_ALL_VISIBLE` set and VM bits set.
  Index-only scans on the standby can take advantage of these bits.

### VM bit-clear is implicit

There is no `XLOG_HEAP2_VM_CLEAR` record. Bit clears happen as a side
effect of:
- `XLOG_HEAP_INSERT` / `_UPDATE` / `_DELETE` / `_LOCK`
- `XLOG_HEAP2_MULTI_INSERT`

Each of those records' redo function calls `visibilitymap_clear` for
the affected heap block. This piggyback saves one WAL record per heap
mutation.

Why is bit-clear safe to piggyback while bit-set is not? Because:
- Bit-set is a *new* assertion ("nothing un-visible on this page");
  it must be durable in its own right, and the corresponding
  `cutoff_xid` must be remembered for recovery.
- Bit-clear is a *retraction* ("might be un-visible now"); over-clearing
  is safe (just causes a heap fetch), so no special record is needed.

### XLogRecPtrIsInvalid path

A caller of `visibilitymap_set` that passes `XLogRecPtrIsInvalid(recptr) =
true` is asking the function to emit `XLOG_HEAP2_VISIBLE` itself. The
function then constructs the record, registers both buffers, and calls
XLogInsert.

A caller that passes a valid `recptr` is signaling "I just emitted the
heap-side WAL record; please align my VM page LSN to it without emitting
another WAL record". Used by the case where a heap-page operation already
established the LSN and we are catching VM up.

### Cross-references

- `[13 Visibility Map](13_visibility_map.md)` — full VM design.
- `[15 Persistence and WAL Records](15_persistence_and_wal_records.md)` — heap WAL records that
  implicitly clear VM bits.


## Section RM_COMMIT_TS_ID

`RM_COMMIT_TS_ID = "CommitTs"`, redo: `commit_ts_redo`
(`src/backend/access/transam/commit_ts.c:1023`).

### XLOG_COMMIT_TS_ZEROPAGE  (info 0x00)

- **Header**: `commit_ts.h:46`.
- **Payload**: `int64 pageno`.
- **Emitter**: `ExtendCommitTs()` (commit_ts.c) when `GetNewTransactionId`
  advances onto a fresh CommitTs page.
- **Redo**: `commit_ts_redo`:
  1. `slot = SimpleLruZeroPage(CommitTsCtl, pageno)`.
  2. `SimpleLruWritePage(CommitTsCtl, slot)`.
- **Makes durable**: zeroing of a fresh `pg_commit_ts` page (covering 819
  XIDs).
- **Full-page image**: not applicable.
- **Standby effects**: new zero page on disk; no caches affected.

### XLOG_COMMIT_TS_TRUNCATE  (info 0x10)

- **Header**: `commit_ts.h:47`.
- **Payload**:
  ```c
  typedef struct xl_commit_ts_truncate
  {
      int64         pageno;
      TransactionId oldestXid;
  } xl_commit_ts_truncate;
  ```
- **Emitter**: `TruncateCommitTs(oldestXact)` from `vac_truncate_clog`.
- **Redo**:
  1. `SetCommitTsLimit(xlrec.oldestXid, GetNextXidAndEpoch().xid)` —
     update `oldestCommitTsXid`.
  2. `SimpleLruTruncate(CommitTsCtl, xlrec.pageno)`.
- **Makes durable**: pg_commit_ts truncation, advancing
  `oldestCommitTsXid`.
- **Standby effects**: shrinks pg_commit_ts directory; the
  `pg_xact_commit_timestamp(xid < oldest)` query on the standby will
  return NULL.

### XLOG_COMMIT_TS_SETTS  (info 0x40)

The exact info-byte value depends on the version; in current code, this
record is emitted by `TransactionTreeSetCommitTsData` only when
`nodeid` is non-default (so a logical-replication subscriber can attribute
the commit to a specific origin).

- **Payload**:
  ```c
  typedef struct xl_commit_ts_set
  {
      TimestampTz   timestamp;
      RepOriginId   nodeid;
      TransactionId mainxid;
      /* TransactionId subxids[]; — variable length */
  } xl_commit_ts_set;
  ```
- **Emitter**: `TransactionTreeSetCommitTsData` (commit_ts.c).
- **Redo**: re-runs the same `TransactionIdSetCommitTs` writes for
  every (xid, subxids[]) using the embedded timestamp + nodeid.
- **Makes durable**: per-XID commit timestamp + RepOriginId.

### When CommitTs data does NOT need its own WAL record

For the common case (no replication origin), the timestamp is embedded in
`xl_xact_commit::xact_time`. `xact_redo_commit` calls
`TransactionTreeSetCommitTsData` with that timestamp during redo, which
writes the SLRU entry. So the standalone XLOG_COMMIT_TS_SETTS record is
the rare path; it carries information that XACT_COMMIT alone cannot
(specifically, a non-default RepOriginId).

### Cross-references

- `[11 Commit Timestamps](11_commit_timestamps.md)` — full CommitTs design.
- `[19 SLRU Users Catalog § CommitTs](19_slru_users_catalog.md)` — pg_commit_ts directory.


## Section RM_DBASE_ID and RM_TBLSPC_ID — database and tablespace

### RM_DBASE_ID — Database

`RM_DBASE_ID = "Database"`, redo: `dbase_redo`
(`src/backend/commands/dbcommands.c`).

#### XLOG_DBASE_CREATE_FILE_COPY  (info 0x00)

- **Header**: `dbcommands_xlog.h:21`.
- **Payload**:
  ```c
  typedef struct xl_dbase_create_file_copy_rec
  {
      Oid db_id;
      Oid tablespace_id;
      Oid src_db_id;
      Oid src_tablespace_id;
  } xl_dbase_create_file_copy_rec;
  ```
- **Emitter**: `createdb_failure_callback` path with
  `STRATEGY = FILE_COPY`, the legacy `CREATE DATABASE` strategy.
- **Redo**: copies the source database's directory tree to the new
  location.
- **Makes durable**: CREATE DATABASE via filesystem copy.

#### XLOG_DBASE_CREATE_WAL_LOG  (info 0x10)

- **Header**: `dbcommands_xlog.h:22`.
- **Payload**:
  ```c
  typedef struct xl_dbase_create_wal_log_rec
  {
      Oid db_id;
      Oid tablespace_id;
  } xl_dbase_create_wal_log_rec;
  ```
- **Emitter**: `createdb_failure_callback` with `STRATEGY = WAL_LOG`
  (the modern default).
- **Redo**: creates the new database directory; subsequent per-relation
  WAL records replicate the actual page contents.
- **Makes durable**: the new database directory's existence; the contents
  come via subsequent records.

#### XLOG_DBASE_DROP  (info 0x20)

- **Header**: `dbcommands_xlog.h:23`.
- **Payload**:
  ```c
  typedef struct xl_dbase_drop_rec
  {
      Oid db_id;
      int ntablespaces;
      Oid tablespace_ids[FLEXIBLE_ARRAY_MEMBER];
  } xl_dbase_drop_rec;
  ```
- **Emitter**: `dropdb`.
- **Redo**: walks `tablespace_ids[]`; for each, removes the
  `<tablespace>/<dbid>` directory recursively.
- **Makes durable**: DROP DATABASE — recursive directory removal across
  every tablespace the database had files in.

### RM_TBLSPC_ID — Tablespace

`RM_TBLSPC_ID = "Tablespace"`, redo: `tblspc_redo`
(`src/backend/commands/tablespace.c`).

#### XLOG_TBLSPC_CREATE  (info 0x00)

- **Header**: `tablespace.h:25`.
- **Payload**:
  ```c
  typedef struct xl_tblspc_create_rec
  {
      Oid    ts_id;
      char   ts_path[FLEXIBLE_ARRAY_MEMBER];
  } xl_tblspc_create_rec;
  ```
- **Emitter**: `CreateTableSpace`.
- **Redo**: creates the symlink under `pg_tblspc/<ts_id>` pointing at
  `ts_path`. Creates the per-database subdirectories that already existed
  on the primary (the symlink target on the standby must be writable).
- **Makes durable**: tablespace symlink creation.

#### XLOG_TBLSPC_DROP  (info 0x10)

- **Header**: `tablespace.h:26`.
- **Payload**:
  ```c
  typedef struct xl_tblspc_drop_rec
  {
      Oid ts_id;
  } xl_tblspc_drop_rec;
  ```
- **Emitter**: `DropTableSpace`.
- **Redo**: removes the `pg_tblspc/<ts_id>` symlink. The standby is
  expected to have already removed any per-database directories under
  the target via DROP DATABASE replays.
- **Makes durable**: tablespace removal.

### Implicit catalog effects

These records do NOT go through pg_catalog tables on the standby —
they directly manipulate filesystem state. The pg_database / pg_tablespace
catalog rows are written via ordinary heap WAL on the primary, and the
standby applies those via heap_xlog_insert / heap_xlog_delete in the
normal way. The XLOG_DBASE_* / XLOG_TBLSPC_* records are *additional*
records that handle the filesystem side (which heap WAL alone cannot
express, since these are directory-level operations).

### Cross-references

- `[15 Persistence and WAL Records](15_persistence_and_wal_records.md)` — overview of all metadata
  records.
- `[18 Catalog Inventory § Core Relations](18_catalog_inventory.md)` — pg_database, pg_tablespace.


---

[Up: index.md](index.md)  |  [Prev: 19 SLRU Users Catalog](19_slru_users_catalog.md)  |  [Next: 21 Deep Dives](21_deep_dives.md)
