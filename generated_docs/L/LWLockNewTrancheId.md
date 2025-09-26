# LWLockNewTrancheId

## Location
src/backend/storage/lmgr/lwlock.c: 606 - 629

## Overview
Allocates a new unique tranche ID for dynamically created LWLock tranches, using a thread-safe counter stored in shared memory.

## Definition


## Detailed Description
LWLockNewTrancheId provides a thread-safe mechanism for allocating unique tranche IDs for LWLocks that are created dynamically at runtime. The function accesses a shared counter that is stored in shared memory just before the MainLWLockArray.

The counter is initialized to LWTRANCHE_FIRST_USER_DEFINED during the CreateLWLocks phase and is incremented atomically each time this function is called. The function uses spinlock protection (ShmemLock) to ensure that concurrent calls from different processes receive unique IDs without conflicts.

This function is primarily used by extensions and test modules that need to create their own LWLock tranches dynamically, rather than using the static named tranche mechanism provided by RequestNamedLWLockTranche(). Each allocated ID can then be used with LWLockRegisterTranche() to associate a descriptive name with the tranche for debugging purposes.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire: Acquires the shared memory spinlock for atomic access
  - SpinLockRelease: Releases the shared memory spinlock
- Global variables accessed:
  - MainLWLockArray: Used to locate the counter stored just before the array
  - ShmemLock: Spinlock protecting the shared counter
- Called from:
  - InitializeLWLocks: During named tranche initialization
  - Various test modules: test_dsa, test_dsm_registry, test_radixtree, test_slru, test_tidstore
  - Extensions needing dynamic tranche allocation

## Notes and Other Information
- The counter is stored at (MainLWLockArray - sizeof(int)) in shared memory
- The function is thread-safe and can be called concurrently from multiple processes
- Tranche IDs start from LWTRANCHE_FIRST_USER_DEFINED and increment sequentially
- Once allocated, tranche IDs are never reused or freed
- Extensions should typically call LWLockRegisterTranche() after getting a new ID to provide a descriptive name
- The function provides no mechanism to deallocate or reuse tranche IDs
- Used extensively by PostgreSQL's test infrastructure for creating isolated lock tranches during testing