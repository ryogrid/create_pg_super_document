# ReorderBufferGetOldestXmin

## Location
[src/backend/replication/logical/reorderbuffer.c:1068-1082](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L1068-L1082)

## Overview
Returns the oldest Xmin value from base snapshots in the reorder buffer, representing the oldest possibly running transaction ID from the perspective of stored snapshots.

## Definition
```c
TransactionId ReorderBufferGetOldestXmin(ReorderBuffer *rb)
```

## Detailed Description
ReorderBufferGetOldestXmin determines the oldest transaction ID (Xmin) that might still be running from the perspective of snapshots stored in the reorder buffer. This function is critical for snapshot management and transaction visibility in logical replication.

The function works by examining the base snapshots of transactions ordered by their base_snapshot_lsn. Since snapshots are assigned monotonically, the transaction with the minimal base_snapshot_lsn will have the oldest Xmin value. This Xmin represents the oldest transaction that was potentially still running when the snapshot was taken.

The returned value is used to determine transaction visibility boundaries and is essential for maintaining proper snapshot isolation during logical replication processing. If no transactions with base snapshots exist in the buffer, the function returns InvalidTransactionId.

## Parameters / Member Variables
- `rb`: Pointer to a ReorderBuffer structure containing transactions with associated base snapshots

## Dependencies
- Functions called/Symbols referenced:
  - AssertTXNLsnOrder (validates LSN ordering in the buffer)
  - dlist_is_empty (checks if the base snapshot transaction list is empty)
  - dlist_head_element (retrieves the transaction with the earliest base snapshot LSN)
- Data structures used:
  - ReorderBuffer
  - ReorderBufferTXN
  - TransactionId
- Called from (representative examples):
  - SnapBuildProcessRunningXacts (at src/backend/replication/logical/snapbuild.c:1319)

## Notes and Other Information
- Returns InvalidTransactionId if no transactions with base snapshots are present
- The function relies on the monotonic assignment of snapshots to ensure correct ordering
- Essential for maintaining proper transaction visibility during logical replication
- The Xmin value affects garbage collection decisions and snapshot validity
- Works specifically with the txns_by_base_snapshot_lsn list, which is ordered by base snapshot LSN values