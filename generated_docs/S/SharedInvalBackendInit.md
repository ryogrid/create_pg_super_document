# SharedInvalBackendInit

## Location
[src/backend/storage/ipc/sinvaladt.c:272-327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/sinvaladt.c#L272-L327)

## Overview
SharedInvalBackendInit initializes a new backend process to participate in the shared invalidation system by registering it in the shared invalidation buffer.

## Definition
void SharedInvalBackendInit(bool sendOnly)

## Detailed Description
This function registers a new backend process with the shared invalidation subsystem, enabling it to send and receive cache invalidation messages. The function performs several critical initialization steps:

1. Validates that MyProcNumber is properly set and within valid bounds
2. Acquires exclusive SInvalWriteLock to prevent concurrent modifications
3. Checks that the assigned process slot is not already in use
4. Adds the process number to the active processes array (pgprocnos)
5. Initializes local transaction ID from the shared state
6. Marks the process as active and sets initial message processing state
7. Registers a cleanup handler to deactivate the process on exit

The sendOnly parameter determines whether the backend can only send invalidation messages (true for auxiliary processes) or can both send and receive them (false for regular backends).

## Parameters / Member Variables
- : Boolean flag indicating whether this backend should only send invalidation messages (true) or can both send and receive them (false)

## Dependencies
- Functions called/Symbols referenced:
  - elog
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [on_shmem_exit](../o/on_shmem_exit.md)
  - [CleanupInvalidationState](../C/CleanupInvalidationState.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
- Data types referenced:
  - [ProcState](../P/ProcState.md)
  - pid_t
  - [SISeg](SISeg.md)
- Constants referenced:
  - ERROR
  - PANIC
  - LW_EXCLUSIVE
- [Variables](../V/Variables.md) referenced:
  - MyProcNumber
  - NumProcStateSlots
  - MyProcPid
  - shmInvalBuffer
  - nextLocalTransactionId
  - SInvalWriteLock
- Called from (representative examples):
  - [InitPostgres](../I/InitPostgres.md)
  - [InitRecoveryTransactionEnvironment](../I/InitRecoveryTransactionEnvironment.md)

## Notes and Other Information
- This function must be called during backend startup after MyProcNumber has been assigned
- The function is not thread-safe and relies on process-level isolation
- Exclusive locking ensures that process registration is atomic and prevents race conditions
- The cleanup handler registration ensures proper resource deallocation even if the process exits abnormally
- Setting nextMsgNum to maxMsgNum means the new backend starts by considering all existing messages as already processed
- The sendOnly parameter is important for auxiliary processes that generate invalidations but don't maintain caches themselves
- Failure cases (slot already in use, invalid MyProcNumber) result in ERROR or PANIC to prevent system corruption

## Simplified Source

```c
// Simplified version of SharedInvalBackendInit
void SharedInvalBackendInit(bool sendOnly) {
    ProcState *stateP;
    pid_t oldPid;
    SISeg *segP = shmInvalBuffer;

    // Validate MyProcNumber is properly set and within bounds
    if (MyProcNumber < 0) {
        elog(ERROR, "MyProcNumber not set");
    }
    if (MyProcNumber >= NumProcStateSlots) {
        elog(PANIC, "MyProcNumber out of bounds");
    }

    stateP = &segP->procState[MyProcNumber];

    // Acquire exclusive lock for process registration
    LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);

    // Check that the slot is not already in use
    oldPid = stateP->procPid;
    if (oldPid != 0) {
        LWLockRelease(SInvalWriteLock);
        elog(ERROR, "sinval slot already in use");
    }

    // Add this process to the active processes array
    shmInvalBuffer->pgprocnos[shmInvalBuffer->numProcs++] = MyProcNumber;

    // Initialize local transaction ID
    nextLocalTransactionId = stateP->nextLXID;

    // Mark this process as active and initialize message state
    stateP->procPid = MyProcPid;
    stateP->nextMsgNum = segP->maxMsgNum;  // Start with all existing messages processed
    stateP->resetState = false;
    stateP->signaled = false;
    stateP->hasMessages = false;
    stateP->sendOnly = sendOnly;

    LWLockRelease(SInvalWriteLock);

    // Register cleanup handler for process exit
    on_shmem_exit(CleanupInvalidationState, PointerGetDatum(segP));
}
```

Key simplifications made:
- Simplified error messages while preserving the essential validation logic
- Consolidated comments to explain the main purpose of each section
- Maintained the exact lock acquisition/release pattern as it's critical
- Preserved all the state initialization steps which are necessary for proper operation
- Removed detailed error message formatting but kept the core safety checks