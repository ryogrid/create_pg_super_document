# PostmasterStateMachine

## Location
src/backend/postmaster/postmaster.c: 3128 - 3421

## Overview
PostmasterStateMachine manages PostgreSQL's master state machine for coordinated shutdown, recovery, and restart operations based on the current process state and shutdown conditions.

## Definition
static void PostmasterStateMachine(void)

## Detailed Description
PostmasterStateMachine is the central coordination function that manages PostgreSQL's postmaster state transitions during shutdown, crash recovery, and restart scenarios. The function operates as a state machine with multiple states: PM_RUN/PM_HOT_STANDBY (normal operation), PM_STOP_BACKENDS (initiating shutdown), PM_WAIT_BACKENDS (waiting for backends to exit), PM_SHUTDOWN/PM_SHUTDOWN_2 (checkpoint and final cleanup), PM_WAIT_DEAD_END (waiting for dead-end processes), and PM_NO_CHILDREN (final state). During shutdown, it coordinates the orderly termination of different process types in sequence: first normal backends, then auxiliary processes like bgwriter and walwriter, followed by walsenders and archiver, and finally dead-end processes. For crash recovery, it handles reinitialization by cleaning up shared memory, removing temporary files, resetting background worker crash times, and restarting the startup process. The function also handles special cases like startup process failure and the restart_after_crash configuration option.

## Parameters / Member Variables
This function takes no parameters and operates on global postmaster state variables including pmState, FatalError, Shutdown, and various process PID tracking variables.

## Dependencies
- Functions called/Symbols referenced:
  - CountChildren
  - ForgetUnstartedBackgroundWorkers
  - SignalSomeChildren
  - signal_child
  - StartChildProcess
  - ConfigurePostmasterWaitSet
  - dlist_is_empty
  - ExitPostmaster
  - RemovePgTempFiles
  - ResetBackgroundWorkerCrashTimes
  - shmem_exit
  - LocalProcessControlFile
  - CreateSharedMemoryAndSemaphores
  - SignalChildren
- Called from (representative examples):
  - process_pm_shutdown_request
  - process_pm_child_exit
  - process_pm_pmsignal

## Notes and Other Information
- Implements PostgreSQL's coordinated shutdown sequence to ensure data consistency
- Handles both normal shutdown and crash recovery scenarios
- Uses state-based logic to manage complex process interdependencies during shutdown
- Critical for preventing conflicts between old and new postmaster instances during restart
- The syslogger process is treated specially and continues running throughout most shutdown phases
- Includes safety assertions to verify expected process states during transitions
- Supports immediate shutdown mode for emergency situations while still maintaining some coordination