# ResetPromoteSignaled

## Location
src/backend/postmaster/startup.c: 294 - 302

## Overview
Resets the promote_signaled flag to false, clearing the promotion signal state in the startup process.

## Definition
```c
void ResetPromoteSignaled(void)
```

## Detailed Description
This function is a simple utility function that resets the static volatile `promote_signaled` flag to false. The `promote_signaled` flag is used in PostgreSQL standby servers to indicate whether a promotion from standby to primary has been signaled. By resetting this flag, the function clears any pending promotion signal, effectively canceling a promotion request or resetting the state after a promotion has been processed.

The function operates on a static volatile sig_atomic_t variable, ensuring thread-safe access to the promotion signal state. This is critical in the startup process where signal handlers and main execution flow need to coordinate safely.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Variables accessed:
  - `promote_signaled` (static volatile sig_atomic_t in startup.c:54)

- Called from:
  - CheckForStandbyTrigger (src/backend/access/transam/xlogrecovery.c:4443)
  - ereport_startup_progress (referenced in src/include/postmaster/startup.h:33)

## Notes and Other Information
- This function is part of PostgreSQL's standby server promotion mechanism
- The `promote_signaled` flag is typically set by signal handlers when a promotion is requested
- Used in conjunction with `IsPromoteSignaled()` to check the current promotion state
- The flag is declared as `volatile sig_atomic_t` to ensure safe access from signal handlers
- Located in the startup process implementation (startup.c:294-297)