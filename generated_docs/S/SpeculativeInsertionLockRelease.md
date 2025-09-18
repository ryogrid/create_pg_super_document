# SpeculativeInsertionLockRelease

## Location
[src/backend/storage/lmgr/lmgr.c:804-819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L804-L819)

## Overview
Releases a speculative insertion lock that was previously acquired, indicating that the speculative insertion operation has completed.

## Definition
```c
void SpeculativeInsertionLockRelease(TransactionId xid)
```

## Detailed Description
This function is the counterpart to SpeculativeInsertionLockAcquire and is responsible for releasing the exclusive lock that was acquired during a speculative insertion operation. When a transaction has completed its decision about whether to commit or abort a speculative insertion, it calls this function to release the lock and allow any waiting transactions to proceed.

The function uses the current value of the global speculativeInsertionToken to construct the appropriate lock tag and then releases the exclusive lock. This allows other transactions that may be waiting on SpeculativeInsertionWait to be awakened and continue their processing.

## Parameters / Member Variables
- `xid`: The transaction ID that previously acquired the speculative insertion lock

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_SPECULATIVE_INSERTION
  - [LockRelease](../L/LockRelease.md)
  - ExclusiveLock
- Called from (representative examples):
  - [ExecInsert](../E/ExecInsert.md) (in nodeModifyTable.c:1142)

## Notes and Other Information
- Must be called after a corresponding SpeculativeInsertionLockAcquire call
- Uses the current value of the global speculativeInsertionToken variable to identify which lock to release
- Essential for completing the speculative insertion protocol and unblocking waiting transactions
- Part of PostgreSQL's mechanism for handling INSERT ... ON CONFLICT operations efficiently
- The lock release will wake up any transactions waiting on this speculative insertion via SpeculativeInsertionWait