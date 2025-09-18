# AdvanceOldestCommitTsXid

## Location
src/backend/access/transam/commit_ts.c: 943 - 976

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
  - LWLockAcquire (with CommitTsLock, LW_EXCLUSIVE)
  - TransactionIdPrecedes
  - LWLockRelease
- Called from (representative examples):
  - commit_ts_redo
  - vac_truncate_clog

## Notes and Other Information
- The function acquires an exclusive lock on CommitTsLock to ensure thread-safe updates to the global oldestCommitTsXid value
- Only advances the ID forward - never moves it backward, maintaining monotonicity
- The check for InvalidTransactionId prevents updating from an uninitialized state
- Part of the commit timestamp subsystem that tracks when transactions were committed for logical replication and other features