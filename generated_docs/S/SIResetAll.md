# SIResetAll

## Location
src/backend/storage/ipc/sinvaladt.c: 700 - 742

## Overview
Forces all active backends into reset state, effectively implementing a cluster-wide cache invalidation when the specific invalidation requirements are unknown.

## Definition
```c
void SIResetAll(void)
```

## Detailed Description
SIResetAll is a drastic but necessary function in PostgreSQL's shared invalidation system that forces all active backends to completely rebuild their caches. This function is used when the system cannot determine exactly what needs to be invalidated, making it equivalent to a cluster-wide InvalidateSystemCaches() operation.

The function was originally implemented as a back-branch-only remedy to avoid WAL format changes while still providing necessary functionality. It operates by acquiring exclusive locks on both the read and write locks, then iterating through all active backends and marking them for reset.

Unlike normal reset operations that only affect backends that have fallen behind, SIResetAll resets even fully caught-up backends. To ensure these backends notice the reset condition, it explicitly sets the hasMessages flag for all reset backends.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease (for SInvalWriteLock and SInvalReadLock exclusive access)
  - SISeg (shared invalidation segment structure)
  - ProcState (per-process state tracking)
  - CLEANUP_MIN (cleanup threshold constant)
- Called from (representative examples):
  - StartupXLOG (during recovery operations when cache coherency is uncertain)
  - Critical system state transitions where comprehensive cache invalidation is required

## Notes and Other Information
- Acquires exclusive locks on both SInvalWriteLock and SInvalReadLock for atomic operation
- Skips sendOnly backends since they don't maintain caches that need invalidation
- Sets both resetState and hasMessages flags to ensure backends notice the reset condition
- Updates segP->minMsgNum to segP->maxMsgNum, effectively marking all messages as consumed
- Resets nextThreshold to CLEANUP_MIN to optimize subsequent cleanup operations
- Originally implemented as a back-branch solution to avoid WAL format changes
- Equivalent to cluster-wide InvalidateSystemCaches() operation
- Used in scenarios where precise invalidation tracking is not possible or reliable
- Forces complete cache rebuilds across all backends, impacting performance but ensuring correctness