# StandbyReleaseLockTree

## Location
[src/backend/storage/ipc/standby.c:1091-1104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L1091-L1104)

## Overview
StandbyReleaseLockTree releases all AccessExclusiveLocks held by a transaction tree (main transaction and its subtransactions) during WAL replay in hot standby mode.

## Definition

```c
void
StandbyReleaseLockTree(TransactionId xid, int nsubxids, TransactionId *subxids)
```
## Detailed Description
This function is called during WAL replay of COMMIT/ROLLBACK operations when PostgreSQL is running in hot standby mode. It systematically releases all AccessExclusiveLocks that were acquired by a transaction tree, ensuring that locks held by both the main transaction and all its subtransactions are properly released from the RecoveryLockXidHash.

The function operates by first releasing locks for the main transaction, then iterating through all subtransactions to release their locks as well. This ensures complete cleanup of the lock tree structure during transaction completion in standby recovery.

## Parameters / Member Variables
- `xid`: The main transaction ID whose locks should be released
- `nsubxids`: The number of subtransactions in the transaction tree
- `*subxids`: Array of subtransaction IDs whose locks should also be released
## Dependencies
- Functions called/Symbols referenced:
  - [StandbyReleaseLocks](StandbyReleaseLocks.md)
- Called from (representative examples):
  - [RecoverPreparedTransactions](../R/RecoverPreparedTransactions.md) (src/backend/access/transam/twophase.c:2149)
  - [xact_redo_commit](../x/xact_redo_commit.md) (src/backend/access/transam/xact.c:6146)
  - [xact_redo_abort](../x/xact_redo_abort.md) (src/backend/access/transam/xact.c:6269)

## Notes and Other Information
- This function is only used during hot standby recovery operations
- It ensures that both main transaction and subtransaction locks are properly released
- The function maintains consistency between primary and standby servers by properly managing lock state during WAL replay
- Located in src/backend/storage/ipc/standby.c:1091-1104

## Simplified Source

```c
// Simplified version of StandbyReleaseLockTree
void StandbyReleaseLockTree(TransactionId xid, int nsubxids, TransactionId *subxids) {
    int i;

    // Release locks for main transaction
    StandbyReleaseLocks(xid);

    // Release locks for all subtransactions
    for (i = 0; i < nsubxids; i++) {
        StandbyReleaseLocks(subxids[i]);
    }
}
```

Key simplifications made:
- Straightforward function with minimal optimization needed
- Clear separation of main transaction and subtransaction processing
- Preserved the essential lock release logic for transaction trees
- Maintained the simple iterative approach for processing subtransactions