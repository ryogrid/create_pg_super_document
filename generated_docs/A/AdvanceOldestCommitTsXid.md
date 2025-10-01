# AdvanceOldestCommitTsXid

## Location
[src/backend/access/transam/commit_ts.c:943-976](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L943-L976)

## Overview
Advances the oldest commit timestamp transaction ID that can be consulted, ensuring that commit timestamp tracking maintains consistency by moving the boundary forward.

## Definition
```c
void AdvanceOldestCommitTsXid(TransactionId oldestXact)
```

## Detailed Description
This function moves forward the oldest commit timestamp transaction ID (oldestCommitTsXid) that can be consulted. It's used to maintain the validity boundary for commit timestamp queries by ensuring that the oldest tracked transaction ID advances monotonically. The function only advances the ID if the new value is greater than the current one, preventing backward movement. This is crucial for commit timestamp SLRU (Simple LRU) management and ensuring that old commit timestamp data can be safely truncated.

## Parameters / Member Variables
- `oldestXact`: The new oldest transaction ID that should be set as the boundary for commit timestamp consultation

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (with CommitTsLock, LW_EXCLUSIVE)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [LWLockRelease](../L/LWLockRelease.md)
- Called from (representative examples):
  - [commit_ts_redo](../c/commit_ts_redo.md)
  - [vac_truncate_clog](../v/vac_truncate_clog.md)

## Notes and Other Information
- The function acquires an exclusive lock on CommitTsLock to ensure thread-safe updates to the global oldestCommitTsXid value
- Only advances the ID forward - never moves it backward, maintaining monotonicity
- The check for InvalidTransactionId prevents updating from an uninitialized state
- Part of the commit timestamp subsystem that tracks when transactions were committed for logical replication and other features

## Simplified Source

```c
void AdvanceOldestCommitTsXid(TransactionId oldestXact)
{
    // Acquire exclusive lock to protect shared state
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);

    // Only advance if current value is valid and new value is newer
    if (TransamVariables->oldestCommitTsXid != InvalidTransactionId &&
        TransactionIdPrecedes(TransamVariables->oldestCommitTsXid, oldestXact))
        TransamVariables->oldestCommitTsXid = oldestXact;

    LWLockRelease(CommitTsLock);
}
```