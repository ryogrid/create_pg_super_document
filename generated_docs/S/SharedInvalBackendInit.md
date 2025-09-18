# SharedInvalBackendInit

## Location
src/backend/storage/ipc/sinvaladt.c: 272 - 327

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
  - LWLockAcquire
  - LWLockRelease
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