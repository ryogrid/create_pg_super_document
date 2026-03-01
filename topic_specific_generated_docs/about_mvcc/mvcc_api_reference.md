# MVCC API Reference

> MVCC Documentation > API Reference

---

Function signatures grouped by subsystem. All signatures verified against PostgreSQL 17.6 source.

## Transaction Lifecycle (`src/backend/access/transam/xact.c`)

```c
static void StartTransaction(void);
static void CommitTransaction(void);
static void AbortTransaction(void);
static TransactionId RecordTransactionCommit(void);
static void RecordTransactionAbort(int nrels, RelFileLocator *rels);
bool TransactionIdIsCurrentTransactionId(TransactionId xid);
CommandId GetCurrentCommandId(bool used);
TransactionId GetCurrentTransactionId(void);
TransactionId GetCurrentTransactionIdIfAny(void);
```

## XID Allocation (`src/backend/access/transam/varsup.c`)

```c
FullTransactionId GetNewTransactionId(bool isSubXact);
void SetTransactionIdLimit(TransactionId oldest_datfrozenxid, Oid oldest_datoid);
```

## Subtransactions (`src/backend/access/transam/subtrans.c`)

```c
void SubTransSetParent(TransactionId xid, TransactionId parent);
TransactionId SubTransGetParent(TransactionId xid);
TransactionId SubTransGetTopmostTransaction(TransactionId xid);
```

## Tuple Operations (`src/backend/access/heap/heapam.c`)

```c
void heap_insert(Relation relation, HeapTuple tup, CommandId cid,
                 int options, BulkInsertState bistate);

TM_Result heap_update(Relation relation, ItemPointer otid, HeapTuple newtup,
                      CommandId cid, Snapshot crosscheck, bool wait,
                      TM_FailureData *tmfd, LockTupleMode *lockmode,
                      TU_UpdateIndexes *update_indexes);

TM_Result heap_delete(Relation relation, ItemPointer tid,
                      CommandId cid, Snapshot crosscheck, bool wait,
                      TM_FailureData *tmfd, bool changingPart);

TM_Result heap_lock_tuple(Relation relation, HeapTuple tuple,
                          CommandId cid, LockTupleMode mode,
                          LockWaitPolicy wait_policy, bool follow_updates,
                          Buffer *buffer, TM_FailureData *tmfd);

bool heap_prepare_freeze_tuple(HeapTupleHeader tuple,
                               const struct VacuumCutoffs *cutoffs,
                               HeapPageFreeze *pagefrz,
                               HeapTupleFreeze *frz, bool *totally_frozen);
```

## Visibility Functions (`src/backend/access/heap/heapam_visibility.c`)

```c
bool HeapTupleSatisfiesVisibility(HeapTuple htup, Snapshot snapshot,
                                  Buffer buffer);

/* Internal dispatch targets (static): */
static bool HeapTupleSatisfiesMVCC(HeapTuple htup, Snapshot snapshot,
                                   Buffer buffer);
static bool HeapTupleSatisfiesSelf(HeapTuple htup, Snapshot snapshot,
                                   Buffer buffer);
static bool HeapTupleSatisfiesDirty(HeapTuple htup, Snapshot snapshot,
                                    Buffer buffer);
static TM_Result HeapTupleSatisfiesUpdate(HeapTuple htup, CommandId curcid,
                                          Buffer buffer);
static HTSV_Result HeapTupleSatisfiesVacuumHorizon(HeapTuple htup, Buffer buffer,
                                                    TransactionId *dead_after);
static bool HeapTupleSatisfiesNonVacuumable(HeapTuple htup, Snapshot snapshot,
                                            Buffer buffer);
static bool HeapTupleSatisfiesHistoricMVCC(HeapTuple htup, Snapshot snapshot,
                                           Buffer buffer);

static inline void SetHintBits(HeapTupleHeader tuple, Buffer buffer,
                                uint16 infomask, TransactionId xid);
```

## Snapshot Construction (`src/backend/storage/ipc/procarray.c`)

```c
Snapshot GetSnapshotData(Snapshot snapshot);
static bool GetSnapshotDataReuse(Snapshot snapshot);

bool TransactionIdIsInProgress(TransactionId xid);
void ProcArrayEndTransaction(PGPROC *proc, TransactionId latestXid);
static void ProcArrayEndTransactionInternal(PGPROC *proc, TransactionId latestXid);
static void ProcArrayGroupClearXid(PGPROC *proc, TransactionId latestXid);

TransactionId GetOldestNonRemovableTransactionId(Relation rel);
TransactionId GetOldestActiveTransactionId(void);
bool GlobalVisTestIsRemovableXid(GlobalVisState *state, TransactionId xid);

void ProcArrayAdd(PGPROC *proc);
void ProcArrayRemove(PGPROC *proc, TransactionId latestXid);
void CreateSharedProcArray(void);
```

## Snapshot Lifecycle (`src/backend/utils/time/snapmgr.c`)

```c
Snapshot GetTransactionSnapshot(void);
Snapshot GetLatestSnapshot(void);
Snapshot GetCatalogSnapshot(Oid relid);
Snapshot GetNonHistoricCatalogSnapshot(Oid relid);

void PushActiveSnapshot(Snapshot snapshot);
void PushCopiedSnapshot(Snapshot snapshot);
void PopActiveSnapshot(void);
Snapshot GetActiveSnapshot(void);
bool ActiveSnapshotSet(void);

Snapshot RegisterSnapshot(Snapshot snapshot);
void UnregisterSnapshot(Snapshot snapshot);
static Snapshot CopySnapshot(Snapshot snapshot);

void AtEOXact_Snapshot(bool isCommit, bool resetXmin);

bool XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot);
```

## CLOG Status (`src/backend/access/transam/transam.c`)

```c
bool TransactionIdDidCommit(TransactionId transactionId);
bool TransactionIdDidAbort(TransactionId transactionId);
XLogRecPtr TransactionIdGetCommitLSN(TransactionId xid);

void TransactionIdCommitTree(TransactionId xid, int nxids, TransactionId *xids);
void TransactionIdAbortTree(TransactionId xid, int nxids, TransactionId *xids);
void TransactionIdAsyncCommitTree(TransactionId xid, int nxids,
                                  TransactionId *xids, XLogRecPtr lsn);

bool TransactionIdPrecedes(TransactionId id1, TransactionId id2);
bool TransactionIdFollows(TransactionId id1, TransactionId id2);
bool TransactionIdPrecedesOrEquals(TransactionId id1, TransactionId id2);
bool TransactionIdFollowsOrEquals(TransactionId id1, TransactionId id2);
```

## CLOG Page Management (`src/backend/access/transam/clog.c`)

```c
void TransactionIdSetTreeStatus(TransactionId xid, int nsubxids,
                                TransactionId *subxids, XidStatus status,
                                XLogRecPtr lsn);
static void TransactionIdSetPageStatus(TransactionId xid, int nsubxids,
                                       TransactionId *subxids, XidStatus status,
                                       XLogRecPtr lsn, int pageno, bool all_xact_same_page);
XidStatus TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn);
```

## VACUUM (`src/backend/commands/vacuum.c`, `src/backend/access/heap/vacuumlazy.c`)

```c
/* Cutoff computation */
bool vacuum_get_cutoffs(Relation rel, const VacuumParams *params,
                        struct VacuumCutoffs *cutoffs);

/* Heap scanning and pruning */
static void lazy_scan_heap(LVRelState *vacrel);
static int lazy_scan_prune(LVRelState *vacrel, Buffer buf,
                           BlockNumber blkno, Page page,
                           Buffer vmbuffer,
                           bool all_visible_according_to_vm,
                           bool *has_lpdead_items);
static void lazy_vacuum_heap_rel(LVRelState *vacrel);

/* CLOG truncation */
void vac_truncate_clog(TransactionId frozenXID, MultiXactId minMulti,
                       TransactionId lastSaneFrozenXid,
                       MultiXactId lastSaneMinMulti);
```

## Page Pruning (`src/backend/access/heap/pruneheap.c`)

```c
void heap_page_prune_and_freeze(Relation relation, Buffer buffer,
                                GlobalVisState *vistest,
                                int options,
                                struct VacuumCutoffs *cutoffs,
                                PruneFreezeResult *presult,
                                PruneReason reason,
                                OffsetNumber *off_loc,
                                TransactionId *new_relfrozen_xid,
                                MultiXactId *new_relmin_mxid);

void heap_page_prune_opt(Relation relation, Buffer buffer);
```

## Predicate Locking / SSI (`src/backend/storage/lmgr/predicate.c`)

```c
void CheckForSerializableConflictIn(Relation relation, ItemPointer tid,
                                    BlockNumber blkno);
void CheckForSerializableConflictOut(Relation relation, TransactionId xid,
                                     Snapshot snapshot);
void PreCommit_CheckForSerializationFailure(void);
```

---

Previous: [Quick Reference](mvcc_quick_reference.md)
