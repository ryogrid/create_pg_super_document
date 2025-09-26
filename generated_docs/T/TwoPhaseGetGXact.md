# TwoPhaseGetGXact

## Location
[src/backend/access/transam/twophase.c:800-851](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L800-L851)

## Overview
TwoPhaseGetGXact retrieves the GlobalTransaction structure for a prepared transaction identified by its transaction ID (XID).

## Definition

```c
static GlobalTransaction
TwoPhaseGetGXact(TransactionId xid, bool lock_held)
```
## Detailed Description
TwoPhaseGetGXact is a static function that searches for and returns the GlobalTransaction structure corresponding to a given transaction ID. The function implements a simple caching mechanism to optimize repeated lookups of the same XID, which is common during recovery, COMMIT PREPARED, and ABORT PREPARED operations. It can operate with or without acquiring TwoPhaseStateLock, depending on whether the caller already holds the lock. The function performs a linear search through the prepared transactions array and throws an error if the requested XID is not found.

## Parameters / Member Variables
- : The TransactionId to search for in the prepared transactions list
- : Boolean flag indicating whether the caller already holds TwoPhaseStateLock

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md) (for lock assertion)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (for shared lock management)
  - elog (for error reporting)
- Data structures accessed:
  - TwoPhaseState (global two-phase commit state)
  - [GlobalTransaction](../G/GlobalTransaction.md) (transaction structure type)
  - Static cache variables: cached_xid, cached_gxact
- Called from:
  - [TwoPhaseGetDummyProcNumber](TwoPhaseGetDummyProcNumber.md)
  - [TwoPhaseGetDummyProc](TwoPhaseGetDummyProc.md)

## Notes and Other Information
- Implements a simple cache to optimize repeated lookups of the same XID
- Cache is particularly useful during recovery operations and prepared transaction completion
- Performs linear search through the prepared transactions array  
- Requires either caller to hold TwoPhaseStateLock or will acquire it in shared mode
- Throws ERROR if the specified XID is not found in prepared transactions
- Static function - only used internally within the two-phase commit subsystem
- The lock_held parameter allows for optimization when caller already has the necessary lock