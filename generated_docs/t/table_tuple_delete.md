# table_tuple_delete

## Location
[src/include/access/tableam.h:1492-1535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1492-L1535)

## Overview
Deletes a single tuple from a table, providing low-level tuple deletion functionality with support for concurrent update handling and MVCC visibility checks.

## Definition
```c
static inline TM_Result
table_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
                   Snapshot snapshot, Snapshot crosscheck, bool wait,
                   TM_FailureData *tmfd, bool changingPart)
```

## Detailed Description
This function provides the core interface for deleting a single tuple from a table. It serves as a wrapper around the table access method's tuple_delete operation, allowing different storage engines to implement their own deletion strategies while maintaining a consistent interface.

The function handles complex scenarios including:
- MVCC visibility checks using snapshots
- Concurrent update detection and resolution
- Cross-checking against additional snapshots for consistency
- Partition key updates that result in tuple movement
- Wait/no-wait semantics for conflicting transactions

The function is designed to be called by higher-level deletion routines rather than directly by most application code, as it requires careful handling of concurrent update conditions.

## Parameters / Member Variables
- `rel`: The relation (table) from which the tuple will be deleted (caller must hold suitable lock)
- `tid`: ItemPointer (TID) identifying the specific tuple to delete
- `cid`: Command ID for visibility testing and stored in cmax if deletion succeeds
- `snapshot`: Snapshot used for visibility testing
- `crosscheck`: Additional snapshot for consistency checking (can be InvalidSnapshot)
- `wait`: Whether to wait for conflicting updates to commit/abort (true) or return immediately (false)
- `tmfd`: Output parameter filled with failure details in case of conflicts
- `changingPart`: Output parameter indicating if tuple is being moved to another partition

## Dependencies
- Functions called/Symbols referenced:
  - rel->rd_tableam->tuple_delete (table access method function pointer)
- Types referenced:
  - CommandId
  - [TM_FailureData](../T/TM_FailureData.md)
  - TM_Result
- Called from (representative examples):
  - [simple_table_tuple_delete](../s/simple_table_tuple_delete.md) (in src/backend/access/table/tableam.c:296)
  - [ExecDeleteAct](../E/ExecDeleteAct.md) (in src/backend/executor/nodeModifyTable.c:1374)

## Notes and Other Information
- Do not call this function directly unless prepared to handle concurrent-update conditions; use simple_table_tuple_delete instead
- Return value TM_Ok indicates successful deletion
- Failure return codes include TM_SelfModified, TM_Updated, and TM_BeingModified
- In failure cases, the tmfd parameter is filled with tuple's t_ctid, t_xmax, and t_cmax when available
- This is an inline function that delegates to the table access method's specific implementation
- Proper locking of the relation is the caller's responsibility
- The changingPart parameter helps distinguish between regular deletions and partition key updates

## Simplified Source

```c
static inline TM_Result
table_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
                   Snapshot snapshot, Snapshot crosscheck, bool wait,
                   TM_FailureData *tmfd, bool changingPart)
{
    // Delegate to the table access method's delete implementation
    return rel->rd_tableam->tuple_delete(rel, tid, cid,
                                        snapshot, crosscheck,
                                        wait, tmfd, changingPart);
}
```