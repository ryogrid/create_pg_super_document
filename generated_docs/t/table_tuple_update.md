# table_tuple_update

## Location
[src/include/access/tableam.h:1536-1580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1536-L1580)

## Overview
Updates a single tuple in a table by replacing an existing tuple with new data, handling concurrent update conditions and maintaining MVCC consistency.

## Definition
```c
static inline TM_Result
table_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
                   CommandId cid, Snapshot snapshot, Snapshot crosscheck,
                   bool wait, TM_FailureData *tmfd, LockTupleMode *lockmode,
                   TU_UpdateIndexes *update_indexes)
```

## Detailed Description
This function provides the core interface for updating a single tuple in a table. It replaces an existing tuple (identified by its TID) with new tuple data, handling complex scenarios related to MVCC, concurrent access, and index maintenance.

Key functionality includes:
- MVCC visibility checks using snapshots
- Concurrent update detection and conflict resolution
- HOT (Heap-Only Tuple) update optimization when possible
- Index maintenance determination
- Lock mode management
- TOAST handling for variable-length attributes

On successful update, the function updates the slot's metadata including the new TID and HEAP_ONLY_TUPLE flag status. The function serves as a wrapper around the table access method's tuple_update implementation, allowing different storage engines to provide optimized update strategies.

## Parameters / Member Variables
- `rel`: The relation (table) to be modified (caller must hold suitable lock)
- `otid`: ItemPointer (TID) of the old tuple to be replaced
- `slot`: TupleTableSlot containing the newly constructed tuple data
- `cid`: Update command ID for visibility testing and stored in cmax/cmin if successful
- `snapshot`: Snapshot used for visibility testing
- `crosscheck`: Additional snapshot for consistency checking (can be InvalidSnapshot)
- `wait`: Whether to wait for conflicting updates to commit/abort or return immediately
- `tmfd`: Output parameter filled with failure details in case of conflicts
- `lockmode`: Output parameter filled with the lock mode acquired on the tuple
- `update_indexes`: Output parameter indicating whether new index entries are required

## Dependencies
- Functions called/Symbols referenced:
  - rel->rd_tableam->tuple_update (table access method function pointer)
- Types referenced:
  - CommandId
  - [TM_FailureData](../T/TM_FailureData.md)
  - [LockTupleMode](../L/LockTupleMode.md)
  - TU_UpdateIndexes
  - TM_Result
- Called from (representative examples):
  - [simple_table_tuple_update](../s/simple_table_tuple_update.md) (in src/backend/access/table/tableam.c:345)
  - [ExecUpdateAct](../E/ExecUpdateAct.md) (in src/backend/executor/nodeModifyTable.c:2135)

## Notes and Other Information
- Do not call this function directly unless prepared to handle concurrent-update conditions; use simple_table_tuple_update instead
- Return value TM_Ok indicates successful update
- Failure return codes include TM_SelfModified, TM_Updated, and TM_BeingModified
- On success, slot's tts_tid and tts_tableOid are updated to reflect the new tuple location
- HEAP_ONLY_TUPLE flag is set in the slot if a HOT update was performed
- TOAST changes in the new tuple are not reflected back into the slot
- In failure cases, tmfd is filled with tuple's t_ctid, t_xmax, and t_cmax when available
- Proper relation locking is the caller's responsibility
- The update_indexes output helps determine if index maintenance is needed

## Simplified Source

```c
static inline TM_Result table_tuple_update(Relation rel, ItemPointer otid,
                                           TupleTableSlot *slot, CommandId cid,
                                           Snapshot snapshot, Snapshot crosscheck,
                                           bool wait, TM_FailureData *tmfd,
                                           LockTupleMode *lockmode,
                                           TU_UpdateIndexes *update_indexes) {
    // Delegate to table access method's update implementation
    // Each storage engine (heap, zheap, etc.) provides optimized update logic
    return rel->rd_tableam->tuple_update(rel, otid, slot, cid, snapshot,
                                        crosscheck, wait, tmfd, lockmode,
                                        update_indexes);
}
```