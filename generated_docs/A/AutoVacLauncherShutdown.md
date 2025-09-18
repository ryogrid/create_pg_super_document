# AutoVacLauncherShutdown

## Location
src/backend/postmaster/autovacuum.c: 775 - 791

## Overview
Performs a clean shutdown of the autovacuum launcher process, logging the shutdown event and clearing the launcher's process ID from shared memory.

## Definition
static void AutoVacLauncherShutdown(void)

## Detailed Description
This function implements the standard shutdown procedure for the autovacuum launcher process. It provides a clean exit mechanism that ensures proper cleanup of shared memory state and logging of the shutdown event. The function is designed to be called when the launcher needs to terminate either due to explicit shutdown requests or configuration changes that disable autovacuuming.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ereport (with DEBUG1 level)
  - [errmsg_internal](../e/errmsg_internal.md)
  - [proc_exit](../p/proc_exit.md)
- Global variables accessed:
  - AutoVacuumShmem->av_launcherpid
- Called from:
  - [HandleAutoVacLauncherInterrupts](../H/HandleAutoVacLauncherInterrupts.md) (on shutdown request - line 744)
  - [HandleAutoVacLauncherInterrupts](../H/HandleAutoVacLauncherInterrupts.md) (when autovacuuming becomes inactive - line 753)

## Notes and Other Information
- This is a static function internal to the autovacuum.c module
- The function logs a DEBUG1 level message for shutdown tracking purposes
- Sets AutoVacuumShmem->av_launcherpid to 0 to indicate no active launcher process
- Uses proc_exit(0) for a normal, successful process termination
- This function provides the primary exit point for the autovacuum launcher process
- The shared memory cleanup is critical for the postmaster to know the launcher has terminated