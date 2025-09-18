# LWLockMode

## Location
src/include/storage/lwlock.h: 119 - 176

## Overview
LWLockMode is an enumeration that defines the different access modes available for PostgreSQL lightweight locks.

## Definition


## Detailed Description
LWLockMode defines the three possible modes for lightweight lock operations in PostgreSQL. The enum provides the foundation for PostgreSQL's reader-writer locking semantics, allowing multiple concurrent readers or a single exclusive writer. The LW_WAIT_UNTIL_FREE mode is a special internal state used for process synchronization when waiting for any type of lock to be released, regardless of the intended access mode.

## Parameters / Member Variables
- `LW_EXCLUSIVE`: Exclusive lock mode - only one process can hold the lock, blocking all other access
- `LW_SHARED`: Shared lock mode - multiple processes can hold shared locks simultaneously, but blocks exclusive access
- `LW_WAIT_UNTIL_FREE`: Special internal mode used in PGPROC->lwWaitMode when waiting for a lock to become completely free

## Dependencies
- Functions called/Symbols referenced:
  - (None - enum definition)
- Called from (representative examples):
  - LWLockAcquire (acquiring locks in specified mode)
  - LWLockConditionalAcquire (attempting non-blocking lock acquisition)
  - LWLockHeldByMeInMode (checking lock ownership in specific mode)
  - LWLockAttemptLock (internal lock acquisition attempts)

## Notes and Other Information
- LW_EXCLUSIVE and LW_SHARED implement standard reader-writer lock semantics
- Multiple shared locks can be held simultaneously by different processes
- Exclusive locks are mutually exclusive with both shared and other exclusive locks
- LW_WAIT_UNTIL_FREE should never be passed as an argument to LWLockAcquire functions
- The mode determines lock compatibility and affects lock queue management
- Essential for PostgreSQL's concurrency control and shared memory protection