# lock_twophase_postabort

## Location
[src/backend/storage/lmgr/lock.c:4413-4436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L4413-L4436)

## Overview
A two-phase commit processing routine that handles lock cleanup during the ROLLBACK PREPARED phase of a prepared transaction.

## Definition

```c
void
lock_twophase_postabort(TransactionId xid, uint16 info,
						void *recdata, uint32 len)
```
## Detailed Description
This function is part of PostgreSQL's two-phase commit protocol implementation for lock management. It handles the cleanup of locks when a prepared transaction is rolled back via ROLLBACK PREPARED. The implementation is notably simple as it delegates all work to lock_twophase_postcommit(), reflecting the fact that from a lock management perspective, both COMMIT PREPARED and ROLLBACK PREPARED require the same cleanup operations - releasing all locks held by the transaction.

The function is called during the post-abort phase of two-phase commit processing, after the transaction has been marked as aborted but before final cleanup is complete.

## Parameters / Member Variables
- `xid`: The transaction ID of the prepared transaction being rolled back
- `info`: Additional information flags for the two-phase commit record
- `recdata`: Pointer to the lock-related data stored in the two-phase commit record
- `len`: Length of the data pointed to by recdata

## Dependencies
- Functions called/Symbols referenced:
  - [lock_twophase_postcommit](lock_twophase_postcommit.md)
- Called from (representative examples):
  - LockHashPartitionLockByProc (via function pointer registration)

## Notes and Other Information
- This function demonstrates that lock cleanup is identical for both commit and abort scenarios in two-phase commit
- The actual lock cleanup logic is implemented in lock_twophase_postcommit()
- This is part of PostgreSQL's resource manager interface for two-phase commit processing
- The function is registered as a callback handler for processing lock-related two-phase commit records during abort

## Simplified Source

```c
void
lock_twophase_postabort(TransactionId xid, uint16 info,
                        void *recdata, uint32 len)
{
    // ROLLBACK PREPARED uses the same lock cleanup as COMMIT PREPARED
    lock_twophase_postcommit(xid, info, recdata, len);
}
```