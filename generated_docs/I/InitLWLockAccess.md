# InitLWLockAccess

## Location
[src/backend/storage/lmgr/lwlock.c:560-575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L560-L575)

## Overview
Initializes backend-local state needed for a process to hold and use LWLocks, primarily for statistics collection when enabled.

## Definition

```c
void
InitLWLockAccess(void)
```
## Detailed Description
InitLWLockAccess is responsible for setting up per-process (backend-local) state required for LWLock operations. Currently, its primary function is to initialize LWLock statistics collection when the LWLOCK_STATS compilation flag is enabled.

This function is called during process initialization to ensure that each backend process has the necessary local state to interact with the shared LWLock infrastructure. Unlike CreateLWLocks and InitializeLWLocks which deal with shared memory structures, this function focuses on process-specific initialization.

When LWLOCK_STATS is not defined (which is the typical case in production builds), this function effectively becomes a no-op, but it still serves as an important hook for any future per-process LWLock initialization that might be needed.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [init_lwlock_stats](../i/init_lwlock_stats.md): Initializes LWLock statistics (only when LWLOCK_STATS is defined)
- Called from:
  - [InitProcess](InitProcess.md): Main backend process initialization
  - [InitAuxiliaryProcess](InitAuxiliaryProcess.md): Auxiliary process initialization

## Notes and Other Information
- This function must be called in every backend process that will use LWLocks
- The function is currently minimal but provides a clean extension point for future per-process LWLock initialization needs
- In debug/development builds with LWLOCK_STATS enabled, this sets up statistics collection infrastructure
- The function is safe to call multiple times in the same process, though typically it's called only once during process startup
- Unlike the shared memory LWLock functions, this operates on process-local state only