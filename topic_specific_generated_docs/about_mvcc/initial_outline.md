# PostgreSQL MVCC Architecture Documentation Outline

## Overview

This document outlines the structure for comprehensive documentation of PostgreSQL's
Multi-Version Concurrency Control (MVCC) system in version 17.6. The MVCC subsystem spans
approximately 15 source files across 4 subdirectories, with 60+ key symbols organized into
7 functional areas.

---

## 1. Transaction Lifecycle

**Coverage depth**: Deep (this is the foundational layer)
**Estimated size**: 8-10 pages

### 1.1 Transaction State Machine
- **Key symbols**: `StartTransaction`, `CommitTransaction`, `AbortTransaction`
- **Source file**: `src/backend/access/transam/xact.c`
- TransactionState enum and state transitions (TRANS_DEFAULT -> TRANS_START -> TRANS_INPROGRESS -> TRANS_COMMIT/TRANS_ABORT)
- Transaction block states (TBLOCK_*) for interactive transaction control
- Memory context lifecycle (TopTransactionContext, CurTransactionContext)

### 1.2 Transaction ID Assignment
- **Key symbols**: `GetNewTransactionId`, `GetCurrentTransactionId`, `TransamVariablesData`
- **Source files**: `src/backend/access/transam/varsup.c`, `src/include/access/transam.h`
- Lazy XID assignment: XIDs not allocated until first write operation
- FullTransactionId (64-bit) vs TransactionId (32-bit) and epoch handling
- XID wraparound protection: xidVacLimit, xidWarnLimit, xidStopLimit, xidWrapLimit
- Special XIDs: InvalidTransactionId (0), BootstrapTransactionId (1), FrozenTransactionId (2)

### 1.3 Transaction Commit Path
- **Key symbols**: `RecordTransactionCommit`, `TransactionIdCommitTree`
- Commit processing phases: pre-commit hooks, WAL record, CLOG update, ProcArray clear
- Synchronous vs asynchronous commit decision
- Two-phase commit (prepared transactions)

### 1.4 Transaction Abort Path
- **Key symbols**: `RecordTransactionAbort`, `TransactionIdAbortTree`
- Abort recording in WAL and CLOG
- Resource cleanup ordering and error handling during abort

### 1.5 Subtransactions
- **Key symbols**: `SubTransSetParent`, `SubTransGetTopmostTransaction`
- **Source file**: `src/backend/access/transam/subtrans.c`
- pg_subtrans SLRU for parent-child mapping
- Subtransaction XID cache in PGPROC (PGPROC_MAX_CACHED_SUBXIDS = 64)
- Overflow handling and its impact on TransactionIdIsInProgress

### Diagrams Needed
- [ ] Transaction state machine diagram (TRANS_* states)
- [ ] Transaction block state diagram (TBLOCK_* states)
- [ ] Commit path sequence diagram showing WAL -> CLOG -> ProcArray ordering

---

## 2. Tuple Versioning

**Coverage depth**: Deep (core MVCC mechanism)
**Estimated size**: 10-12 pages

### 2.1 HeapTupleHeaderData Structure
- **Key symbols**: `HeapTupleHeaderData`, `HeapTupleFields`
- **Source file**: `src/include/access/htup_details.h`
- Physical layout: t_xmin, t_xmax, t_cid/t_xvac union, t_ctid, t_infomask, t_infomask2, t_hoff
- 23-byte fixed header size
- Infomask flags: HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID, HEAP_XMIN_FROZEN, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMAX_LOCK_ONLY, HEAP_UPDATED
- Infomask2 flags: HEAP_HOT_UPDATED, HEAP_ONLY_TUPLE, HEAP_KEYS_UPDATED

### 2.2 Tuple Insertion
- **Key symbols**: `heap_insert`
- **Source file**: `src/backend/access/heap/heapam.c`
- Setting t_xmin to current transaction ID
- Buffer allocation via RelationGetBufferForTuple
- Visibility map handling for all-visible pages
- WAL logging (xl_heap_insert)
- Speculative insertion for ON CONFLICT

### 2.3 Tuple Deletion
- **Key symbols**: `heap_delete`
- Setting t_xmax to current transaction ID (marking, not physically removing)
- HeapTupleSatisfiesUpdate check for concurrent conflicts
- Wait-for-lock semantics when tuple is being updated by another transaction
- WAL logging (xl_heap_delete)

### 2.4 Tuple Update (Multi-Version Creation)
- **Key symbols**: `heap_update`
- Old tuple: set t_xmax, set t_ctid to point to new version
- New tuple: set t_xmin, set HEAP_UPDATED flag
- HOT (Heap-Only Tuple) update optimization:
  - Conditions: no indexed columns changed, new version fits on same page
  - HEAP_HOT_UPDATED and HEAP_ONLY_TUPLE flags
  - HOT chain following via t_ctid
- TOAST handling for oversized attributes

### 2.5 Tuple Locking
- **Key symbols**: `heap_lock_tuple`
- Row-level lock modes: FOR KEY SHARE, FOR SHARE, FOR NO KEY UPDATE, FOR UPDATE
- Encoding locks in t_xmax with HEAP_XMAX_LOCK_ONLY
- MultiXact creation for multiple concurrent lockers
- Lock escalation from shared to exclusive

### 2.6 HOT Chains
- **Key symbols**: `heap_hot_search_buffer`
- Chain traversal: root item -> HEAP_HOT_UPDATED -> HEAP_ONLY_TUPLE -> ... -> current
- Benefits: avoids index updates, enables page-level pruning
- Chain validation: matching xmin/xmax across versions

### 2.7 ComboCID
- Handling of same-transaction insert+delete scenarios
- Mapping local cmin/cmax pairs to combo command IDs
- Backend-local combo CID hash table

### Diagrams Needed
- [ ] HeapTupleHeaderData memory layout diagram
- [ ] Tuple version chain diagram (insert -> update -> delete)
- [ ] HOT chain diagram showing index pointer, root item, and heap-only tuples
- [ ] Infomask bit combinations truth table

---

## 3. Visibility Determination

**Coverage depth**: Very deep (the heart of MVCC)
**Estimated size**: 12-15 pages

### 3.1 Visibility Function Dispatcher
- **Key symbols**: `HeapTupleSatisfiesVisibility`
- **Source file**: `src/backend/access/heap/heapam_visibility.c`
- Routing based on SnapshotType to specific functions
- Common pattern: check xmin first, then xmax

### 3.2 HeapTupleSatisfiesMVCC (Primary Path)
- **Key symbols**: `HeapTupleSatisfiesMVCC`, `XidInMVCCSnapshot`
- Algorithm walkthrough with all branches:
  1. Check xmin committed via hint bits
  2. If not committed: check MOVED_OFF/MOVED_IN, current transaction, XidInMVCCSnapshot
  3. If committed but not frozen: verify not in snapshot
  4. Check xmax: invalid, lock-only, MultiXact, current transaction, in snapshot, committed
- Command ID checks for same-transaction visibility (curcid)
- Critical ordering: XidInMVCCSnapshot before TransactionIdDidCommit

### 3.3 HeapTupleSatisfiesUpdate
- **Key symbols**: `HeapTupleSatisfiesUpdate`
- Return values: HeapTupleMayBeUpdated, HeapTupleInvisible, HeapTupleSelfUpdated, HeapTupleUpdated, HeapTupleBeingUpdated, HeapTupleWouldBlock
- Special handling for speculative insertions
- Integration with row-level locking

### 3.4 HeapTupleSatisfiesVacuum
- **Key symbols**: `HeapTupleSatisfiesVacuum`, `HeapTupleSatisfiesVacuumHorizon`
- HTSV_Result codes: HEAPTUPLE_LIVE, HEAPTUPLE_RECENTLY_DEAD, HEAPTUPLE_DELETE_IN_PROGRESS, HEAPTUPLE_INSERT_IN_PROGRESS, HEAPTUPLE_DEAD
- dead_after TransactionId for recently-dead determination
- OldestXmin cutoff comparison

### 3.5 Other Visibility Functions
- **Key symbols**: `HeapTupleSatisfiesSelf`, `HeapTupleSatisfiesDirty`, `HeapTupleSatisfiesNonVacuumable`, `HeapTupleSatisfiesHistoricMVCC`
- Self: includes current command's changes
- Dirty: includes in-progress transactions (for EvalPlanQual)
- NonVacuumable: GlobalVisState-based pruning check
- HistoricMVCC: inverted snapshot for logical decoding

### 3.6 Hint Bits
- **Key symbols**: `SetHintBits`
- Optimization: avoid repeated CLOG lookups by caching status in t_infomask
- WAL flush safety: commit hint bits only safe when commit LSN <= buffer LSN or WAL flushed
- Abort hint bits: always safe to set (crash implies abort)
- MarkBufferDirtyHint: non-WAL-logged dirty marking

### 3.7 Race Condition Prevention
- Critical ordering: TransactionIdIsInProgress BEFORE TransactionIdDidCommit
- Window where both can return true (between CLOG write and ProcArray clear)
- MVCC snapshots use XidInMVCCSnapshot instead, avoiding the race

### Diagrams Needed
- [ ] HeapTupleSatisfiesMVCC decision tree flowchart
- [ ] Visibility function selection diagram (SnapshotType -> function)
- [ ] Hint bit state machine (no hints -> committed/aborted)
- [ ] Race condition timeline: CLOG write vs ProcArray clear

---

## 4. Snapshot Management

**Coverage depth**: Deep
**Estimated size**: 8-10 pages

### 4.1 SnapshotData Structure
- **Key symbols**: `SnapshotData`, `SnapshotType`
- **Source file**: `src/include/utils/snapshot.h`
- Fields: xmin, xmax, xip[], xcnt, subxip[], subxcnt, suboverflowed, curcid
- xmin optimization: all XIDs < xmin are known visible (no array search needed)
- xmax boundary: all XIDs >= xmax are invisible

### 4.2 Snapshot Construction (GetSnapshotData)
- **Key symbols**: `GetSnapshotData`, `GetSnapshotDataReuse`
- **Source file**: `src/backend/storage/ipc/procarray.c`
- ProcArray scanning: reading dense xids[] array under LW_SHARED lock
- Computing xmin (minimum of all backends' xids and xmins)
- Populating xip[] with in-progress transaction IDs
- Subtransaction handling and suboverflowed flag
- Reuse optimization via xactCompletionCount

### 4.3 Snapshot Lifecycle
- **Key symbols**: `GetTransactionSnapshot`, `PushActiveSnapshot`, `PopActiveSnapshot`, `CopySnapshot`, `RegisterSnapshot`, `AtEOXact_Snapshot`
- **Source file**: `src/backend/utils/time/snapmgr.c`
- Active snapshot stack: push/pop around query execution
- Registered snapshots: reference-counted, heap-tracked for oldest-first ordering
- Isolation level behavior:
  - READ COMMITTED: new snapshot per statement
  - REPEATABLE READ: single snapshot for entire transaction
  - SERIALIZABLE: same as REPEATABLE READ + predicate locking

### 4.4 XidInMVCCSnapshot
- **Key symbols**: `XidInMVCCSnapshot`
- Fast path: xid < xmin -> not in snapshot (committed before snapshot)
- Fast path: xid >= xmax -> in snapshot (started after snapshot)
- Array search: binary search through sorted xip[] array
- Subtransaction handling: search subxip[] or fall through to CLOG if overflowed

### 4.5 Catalog Snapshots
- **Key symbols**: `GetCatalogSnapshot`, `GetNonHistoricCatalogSnapshot`
- Special snapshot handling for system catalog access
- Invalidation and refresh during DDL

### Diagrams Needed
- [ ] SnapshotData field layout and relationship to ProcArray
- [ ] Snapshot lifecycle: construction -> push -> use -> pop -> free
- [ ] XidInMVCCSnapshot decision flow
- [ ] Isolation level snapshot behavior comparison

---

## 5. Concurrency Infrastructure

**Coverage depth**: Deep
**Estimated size**: 10-12 pages

### 5.1 PGPROC and ProcArray
- **Key symbols**: `PGPROC`, `PROC_HDR`, `CreateSharedProcArray`, `ProcArrayAdd`, `ProcArrayRemove`
- **Source files**: `src/include/storage/proc.h`, `src/backend/storage/ipc/procarray.c`
- PGPROC struct: per-backend shared memory with xid, xmin, subxid cache
- Dense mirrored arrays in PROC_HDR for cache-efficient scanning
- pgxactoff indexing: only valid under ProcArrayLock or XidGenLock
- Sorted array maintenance for locality

### 5.2 TransactionIdIsInProgress
- **Key symbols**: `TransactionIdIsInProgress`
- Multi-level optimization:
  1. Check xmin from recent snapshot (fast reject)
  2. Check own transaction
  3. Scan ProcArray main XIDs
  4. Scan cached subtransaction XIDs
  5. Handle subtransaction overflow: pg_subtrans lookup + rescan
- Recovery mode: KnownAssignedXids for hot standby

### 5.3 Transaction Completion Tracking
- **Key symbols**: `ProcArrayEndTransaction`, `ProcArrayGroupClearXid`
- latestCompletedXid advancement
- xactCompletionCount for snapshot reuse detection
- Group XID clearing: batch multiple backends under single lock acquisition

### 5.4 Visibility Horizons
- **Key symbols**: `GetOldestNonRemovableTransactionId`, `GetOldestActiveTransactionId`, `GlobalVisTestIsRemovableXid`
- OldestXmin computation: minimum across all backends' xmins, xids, replication slots
- GlobalVisState: per-relation visibility test cache
- Impact on VACUUM: cannot remove tuples with xmax >= OldestXmin

### 5.5 Predicate Locking (SSI)
- **Key symbols**: `PreCommit_CheckForSerializationFailure`, `CheckForSerializableConflictIn`, `CheckForSerializableConflictOut`
- **Source file**: `src/backend/storage/lmgr/predicate.c`
- Serializable Snapshot Isolation (SSI) theory: detecting dangerous rw-dependency cycles
- SIREAD locks: predicate locks that track what serializable transactions have read
- Conflict detection at write time (ConflictIn) and read time (ConflictOut)
- Commit-time validation: checking for pivot structures

### Diagrams Needed
- [ ] PGPROC structure and its relationship to ProcGlobal dense arrays
- [ ] ProcArray scanning flow for TransactionIdIsInProgress
- [ ] Group XID clearing sequence diagram
- [ ] SSI rw-dependency cycle detection illustration

---

## 6. CLOG (Commit Log)

**Coverage depth**: Moderate-Deep
**Estimated size**: 6-8 pages

### 6.1 CLOG Architecture
- **Source file**: `src/backend/access/transam/clog.c`
- Two-bit per-transaction status: IN_PROGRESS (00), COMMITTED (01), ABORTED (10), SUB_COMMITTED (11)
- SLRU-based storage in pg_xact directory
- Page size: 8KB, 32K transactions per page (256KB segment files)

### 6.2 SLRU Infrastructure
- **Key symbols**: `SimpleLruInit`
- **Source file**: `src/backend/access/transam/slru.c`
- Shared buffer pool with per-buffer LWLocks
- Page-level I/O with fsync management
- Used by: CLOG, pg_subtrans, pg_multixact, pg_commit_ts

### 6.3 Status Lookup
- **Key symbols**: `TransactionIdDidCommit`, `TransactionIdDidAbort`, `TransactionIdGetStatus`
- **Source file**: `src/backend/access/transam/transam.c`
- TransactionIdDidCommit: checks CLOG status, handles SUB_COMMITTED by recursing to parent
- TransactionIdDidAbort: checks for explicit abort only (not crash-implied)
- TransactionIdGetStatus: raw SLRU page read, returns associated LSN

### 6.4 Status Recording
- **Key symbols**: `TransactionIdSetTreeStatus`, `TransactionIdSetPageStatus`, `TransactionIdCommitTree`, `TransactionIdAbortTree`
- Tree status setting: atomicity across pages by setting parent transaction last
- Group commit optimization: batching status updates under single SLRU lock
- WAL interaction: commit LSN recording for hint bit safety

### 6.5 CLOG Truncation
- **Key symbols**: `vac_truncate_clog`
- Driven by VACUUM: scans pg_database for oldest datfrozenxid
- Advances oldestClogXid to prevent lookups of recycled pages
- Coordinated with pg_subtrans and pg_multixact truncation

### 6.6 TransactionIdGetCommitLSN
- **Key symbols**: `TransactionIdGetCommitLSN`
- Returns the LSN sufficient to guarantee commit record is flushed
- Critical for hint bit safety: SetHintBits checks this against buffer LSN

### Diagrams Needed
- [ ] CLOG page layout: 2 bits per XID, page/segment organization
- [ ] SLRU buffer pool architecture
- [ ] Transaction status state machine: IN_PROGRESS -> COMMITTED/ABORTED (with SUB_COMMITTED intermediate)
- [ ] CLOG truncation flow triggered by VACUUM

---

## 7. VACUUM and Garbage Collection

**Coverage depth**: Deep
**Estimated size**: 10-12 pages

### 7.1 VACUUM Overview
- Two-pass strategy: scan (prune+freeze) then vacuum (reclaim)
- Lazy VACUUM vs aggressive VACUUM (anti-wraparound)
- Autovacuum triggering thresholds

### 7.2 Cutoff Computation
- **Key symbols**: `vacuum_get_cutoffs`, `GetOldestNonRemovableTransactionId`
- **Source file**: `src/backend/commands/vacuum.c`
- OldestXmin: tuples with xmax < OldestXmin are definitely dead to all
- FreezeLimit: tuples with xmin < FreezeLimit can be frozen
- MultiXactCutoff: MultiXactIds older than this should be resolved
- Aggressive mode: triggered by age thresholds (vacuum_freeze_table_age)

### 7.3 Page Pruning
- **Key symbols**: `heap_page_prune_and_freeze`, `heap_page_prune_opt`
- **Source file**: `src/backend/access/heap/pruneheap.c`
- HOT chain pruning: removes dead intermediate versions
- Page defragmentation: compacts remaining tuples
- Opportunistic pruning during regular scans (heap_page_prune_opt)
- Integration with freezing in the same pass

### 7.4 Tuple Freezing
- **Key symbols**: `heap_prepare_freeze_tuple`, `heap_execute_freeze_tuple`, `FreezeMultiXactId`
- **Source file**: `src/backend/access/heap/heapam.c`
- Purpose: prevent XID wraparound by replacing old XIDs with FrozenTransactionId
- Freeze plan preparation: analyzes xmin, xmax, xvac against cutoffs
- xmin freezing: set HEAP_XMIN_FROZEN (both COMMITTED and INVALID bits)
- xmax freezing: handle MultiXactId resolution, lock-only preservation
- MultiXact freezing: resolve members, replace with simpler representation

### 7.5 Vacuum Heap Pass
- **Key symbols**: `lazy_vacuum_heap_rel`
- **Source file**: `src/backend/access/heap/vacuumlazy.c`
- Converts LP_DEAD items to LP_UNUSED
- Updates visibility map for all-visible/all-frozen pages
- Truncates trailing empty pages

### 7.6 Conflict Horizon Advancement
- **Key symbols**: `HeapTupleHeaderAdvanceConflictHorizon`
- Tracking the newest committed XID among removed tuples
- Hot standby conflict prevention: ensuring standby queries see consistent data
- Integration with index vacuum operations

### 7.7 CLOG and MultiXact Truncation
- **Key symbols**: `vac_truncate_clog`
- System-wide datfrozenxid/datminmxid advancement
- pg_xact, pg_subtrans, pg_multixact, pg_commit_ts cleanup

### Diagrams Needed
- [ ] VACUUM two-pass flow: scan pass (prune+freeze) then heap vacuum pass
- [ ] Tuple freezing decision tree for xmin and xmax
- [ ] Page pruning before/after: HOT chain removal and defragmentation
- [ ] XID wraparound prevention lifecycle: normal -> warn -> stop -> wrap

---

## Appendices

### A. Data Structure Quick Reference
- HeapTupleHeaderData field map with byte offsets
- SnapshotData field descriptions
- PGPROC MVCC-related fields
- Infomask and infomask2 flag quick reference

### B. Critical Path Summaries
1. **Transaction Commit**: CommitTransaction -> RecordTransactionCommit -> TransactionIdCommitTree -> TransactionIdSetTreeStatus -> ProcArrayEndTransaction
2. **MVCC Visibility Check**: HeapTupleSatisfiesVisibility -> HeapTupleSatisfiesMVCC -> XidInMVCCSnapshot -> TransactionIdDidCommit -> SetHintBits
3. **VACUUM Dead Tuple Removal**: vacuum_get_cutoffs -> heap_page_prune_and_freeze -> HeapTupleSatisfiesVacuumHorizon -> heap_prepare_freeze_tuple -> lazy_vacuum_heap_rel
4. **Snapshot Acquisition**: GetTransactionSnapshot -> GetSnapshotData -> CopySnapshot -> PushActiveSnapshot
5. **Tuple Update**: heap_update -> HeapTupleSatisfiesUpdate -> GetCurrentTransactionId -> CheckForSerializableConflictIn
6. **XID Lifecycle**: GetNewTransactionId -> SubTransSetParent -> TransactionIdIsInProgress -> ProcArrayEndTransaction
7. **CLOG Resolution**: TransactionIdDidCommit -> TransactionIdGetStatus -> TransactionIdGetCommitLSN -> SetHintBits

### C. Source File Index
| File | Functional Area | Key Symbols |
|------|----------------|-------------|
| src/backend/access/heap/heapam.c | tuple, vacuum | heap_insert, heap_delete, heap_update, heap_lock_tuple, heap_prepare_freeze_tuple |
| src/backend/access/heap/heapam_visibility.c | visibility | HeapTupleSatisfiesMVCC, HeapTupleSatisfiesUpdate, HeapTupleSatisfiesVacuum, SetHintBits |
| src/backend/access/heap/pruneheap.c | vacuum | heap_page_prune_and_freeze, heap_page_prune_opt |
| src/backend/access/heap/vacuumlazy.c | vacuum | lazy_vacuum_heap_rel |
| src/backend/access/transam/xact.c | transaction | StartTransaction, CommitTransaction, AbortTransaction, RecordTransactionCommit |
| src/backend/access/transam/varsup.c | transaction | GetNewTransactionId |
| src/backend/access/transam/transam.c | clog | TransactionIdDidCommit, TransactionIdDidAbort |
| src/backend/access/transam/clog.c | clog | TransactionIdSetTreeStatus, TransactionIdGetStatus |
| src/backend/access/transam/subtrans.c | transaction | SubTransSetParent, SubTransGetTopmostTransaction |
| src/backend/access/transam/slru.c | clog | SimpleLruInit |
| src/backend/storage/ipc/procarray.c | concurrency, snapshot | GetSnapshotData, TransactionIdIsInProgress, ProcArrayEndTransaction |
| src/backend/storage/lmgr/predicate.c | concurrency | CheckForSerializableConflictIn, PreCommit_CheckForSerializationFailure |
| src/backend/utils/time/snapmgr.c | snapshot | GetTransactionSnapshot, PushActiveSnapshot, PopActiveSnapshot, XidInMVCCSnapshot |
| src/backend/commands/vacuum.c | vacuum | vacuum_get_cutoffs |
| src/include/access/htup_details.h | tuple | HeapTupleHeaderData, HeapTupleFields, infomask definitions |
| src/include/utils/snapshot.h | snapshot | SnapshotData, SnapshotType |
| src/include/storage/proc.h | concurrency | PGPROC, PROC_HDR |
| src/include/access/transam.h | transaction | TransamVariablesData, special XID constants |

### D. Estimated Documentation Sizes
| Section | Pages | Diagrams | Priority |
|---------|-------|----------|----------|
| 1. Transaction Lifecycle | 8-10 | 3 | High |
| 2. Tuple Versioning | 10-12 | 4 | High |
| 3. Visibility Determination | 12-15 | 4 | Critical |
| 4. Snapshot Management | 8-10 | 4 | High |
| 5. Concurrency Infrastructure | 10-12 | 4 | High |
| 6. CLOG | 6-8 | 4 | Medium |
| 7. VACUUM/Garbage Collection | 10-12 | 4 | High |
| **Total** | **64-79** | **27** | |
