# PostPrepare_Twophase

## Location
[src/backend/access/transam/twophase.c:344-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L344-L358)

## Overview
Completes the two-phase commit preparation process by releasing the lock on the global transaction entry after successful state transfer.

## Definition
void PostPrepare_Twophase(void)

## Detailed Description
This function marks the completion of the two-phase commit preparation phase. It is called after all transaction state has been successfully transferred to the prepared PGPROC entry and the transaction is fully prepared for later commit or rollback operations.

The function performs the final cleanup step in the preparation process by releasing the exclusive lock that the current backend held on the global transaction entry. By setting the locking_backend field to INVALID_PROC_NUMBER, it indicates that no backend currently owns the prepared transaction, making it available for later resolution by the same or different backend process.

This is a critical synchronization point in the two-phase commit protocol, ensuring that the prepared transaction is properly released from the preparing backend while maintaining data consistency through proper locking mechanisms.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (acquires TwoPhaseStateLock in exclusive mode)
  - [LWLockRelease](../L/LWLockRelease.md) (releases TwoPhaseStateLock)
  - INVALID_PROC_NUMBER (constant indicating no backend owns the transaction)
- Global variables accessed:
  - MyLockedGxact (current process's locked global transaction)
  - TwoPhaseStateLock (lightweight lock protecting two-phase state)
- Called from:
  - [RecoverPreparedTransactions](../R/RecoverPreparedTransactions.md) (in twophase.c:2155) - during recovery processing
  - [PrepareTransaction](PrepareTransaction.md) (in xact.c:2697) - during normal transaction preparation

## Notes and Other Information
- Called only after successful completion of state transfer to the prepared PGPROC entry
- The transaction must be in a valid, fully prepared state when this function is called
- Uses exclusive locking to ensure atomic updates to the global transaction state
- After this function completes, the prepared transaction can be committed or rolled back by any backend
- Essential for the distributed transaction protocol - separates transaction preparation from transaction resolution
- The MyLockedGxact is set to NULL to indicate the current process no longer holds any global transaction lock
- Part of the normal two-phase commit flow, complementing the abort handling in AtAbort_Twophase
- Ensures proper resource management and prevents deadlocks in distributed transaction scenarios