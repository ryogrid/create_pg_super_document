# heap_acquire_tuplock

## Location
src/backend/access/heap/heapam.c: 5231 - 5279

## Overview
heap_acquire_tuplock acquires a heavyweight lock on a specific tuple in preparation for acquiring its normal Xmax-based tuple lock, providing different wait policies for lock acquisition scenarios.

## Definition


## Detailed Description
This static function serves as a preparatory step before acquiring tuple locks by first obtaining a heavyweight lock on the target tuple. The function implements three different wait policies: blocking until the lock is available, skipping if the lock cannot be immediately acquired, or throwing an error if the lock is unavailable. 

The function uses an input/output parameter pattern where have_tuple_lock indicates whether the lock has been previously acquired - if true, the function returns immediately without attempting to acquire the lock again. Upon successful lock acquisition, this parameter is set to true to indicate the lock state for subsequent operations.

The function utilizes either LockTupleTuplock for blocking operations or ConditionalLockTupleTuplock for non-blocking attempts, depending on the specified wait policy.

## Parameters / Member Variables
- : The heap relation containing the target tuple
- : ItemPointer (tuple identifier) specifying the exact tuple to lock
- : LockTupleMode specifying the type of lock to acquire
- : LockWaitPolicy determining behavior when lock cannot be immediately acquired
- : Input/output boolean pointer indicating current lock state and updated upon acquisition

## Dependencies
- Functions called/Symbols referenced:
  - LockTupleTuplock
  - ConditionalLockTupleTuplock
  - LockTupleMode (enum)
  - LockWaitPolicy (enum)
  - LockWaitBlock, LockWaitSkip, LockWaitError (enum values)
- Called from (representative examples):
  - heap_delete
  - heap_update
  - heap_lock_tuple

## Notes and Other Information
- This is a static function internal to heapam.c, not exposed in the public API
- Returns false only when wait_policy is Skip and the lock cannot be immediately acquired
- The function implements a safety check by immediately returning true if the lock is already held
- Error reporting uses the standard PostgreSQL ereport mechanism with ERRCODE_LOCK_NOT_AVAILABLE
- The heavyweight tuple lock serves as a prerequisite for the lighter-weight Xmax-based tuple locking mechanism