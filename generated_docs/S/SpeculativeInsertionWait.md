# SpeculativeInsertionWait

## Location
src/backend/storage/lmgr/lmgr.c: 820 - 837

## Overview
Waits for a specified transaction to complete or abort its speculative insertion operation before proceeding.

## Definition
```c
void SpeculativeInsertionWait(TransactionId xid, uint32 token)
```

## Detailed Description
This function implements a waiting mechanism that allows one transaction to wait for another transaction's speculative insertion to complete. It works by attempting to acquire a ShareLock on the same lock tag that the inserting transaction holds with an ExclusiveLock.

The function will block until the inserting transaction releases its exclusive lock (via SpeculativeInsertionLockRelease), at which point this function can acquire the share lock. It immediately releases the share lock after acquiring it, since the purpose is only to wait for the completion of the speculative insertion.

This mechanism is crucial for handling conflicts in speculative insertions, particularly in unique constraint checking and B-tree index operations where one transaction needs to wait for another to decide whether to commit or abort an insertion.

## Parameters / Member Variables
- `xid`: The transaction ID of the transaction performing the speculative insertion to wait for
- `token`: The unique token that identifies the specific speculative insertion within the transaction

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_SPECULATIVE_INSERTION
  - [LockAcquire](../L/LockAcquire.md)
  - ShareLock
  - [LockRelease](../L/LockRelease.md)
- Called from (representative examples):
  - [_bt_doinsert](../b/_bt_doinsert.md) (in nbtinsert.c:225)
  - [check_exclusion_or_unique_constraint](../c/check_exclusion_or_unique_constraint.md) (in execIndexing.c:844)

## Notes and Other Information
- The function includes assertions to ensure the transaction ID is valid and the token is non-zero
- Uses the acquire-then-immediately-release pattern on a ShareLock to implement the waiting behavior
- Essential for preventing race conditions in speculative insertion scenarios
- Commonly used in B-tree index insertion and unique constraint checking
- The token parameter must match the token returned by the corresponding SpeculativeInsertionLockAcquire call
- Will block the calling transaction until the target transaction completes its speculative insertion decision