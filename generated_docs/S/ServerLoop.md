# ServerLoop

## Location
src/backend/postmaster/postmaster.c: 1626 - 1836

## Overview
The main event loop of the PostgreSQL postmaster process that handles client connections, manages background processes, and performs periodic maintenance tasks.

## Definition


## Detailed Description
ServerLoop is the heart of the PostgreSQL postmaster, implementing the main event-driven loop that keeps the database server operational. This function runs continuously until server shutdown, orchestrating all major server operations:

**Core Event Handling:**
- Uses WaitEventSetWait() to efficiently monitor multiple events simultaneously
- Processes latch signals for internal communication (shutdown, reload, child exit, pmsignal)
- Accepts new client connections and forks backend processes via BackendStartup()
- Handles connection requests with high priority, ensuring responsive client service

**Process Management:**
- Monitors and restarts critical background processes (checkpointer, background writer, WAL writer)
- Manages autovacuum launcher based on system state and configuration
- Starts archiver processes when needed
- Coordinates slot synchronization and WAL receiver processes
- Handles background worker lifecycle through maybe_start_bgworkers()

**System Maintenance:**
- Performs periodic lock file validation (every minute) to detect external tampering
- Updates socket file timestamps (every 58 minutes) to prevent cleanup by system tasks
- Manages graceful and immediate shutdown sequences
- Enforces SIGKILL timeout for unresponsive child processes during shutdown

The loop uses DetermineSleepTime() to calculate optimal wait durations, balancing responsiveness with system efficiency.

## Parameters / Member Variables
- No parameters (void function)
- Local variables:
  - : Timestamp for periodic lock file validation
  - : Timestamp for socket file maintenance
  - : Array to store wait events
  - : Count of triggered events

## Dependencies
- Functions called/Symbols referenced:
  - [ConfigurePostmasterWaitSet](../C/ConfigurePostmasterWaitSet.md)
  - WaitEventSetWait
  - [DetermineSleepTime](../D/DetermineSleepTime.md)
  - [ResetLatch](../R/ResetLatch.md)
  - [AcceptConnection](../A/AcceptConnection.md)
  - [BackendStartup](../B/BackendStartup.md)
  - [StartChildProcess](StartChildProcess.md)
  - [SysLogger_Start](SysLogger_Start.md)
  - [MaybeStartWalReceiver](../M/MaybeStartWalReceiver.md)
  - [MaybeStartWalSummarizer](../M/MaybeStartWalSummarizer.md)
  - [MaybeStartSlotSyncWorker](../M/MaybeStartSlotSyncWorker.md)
  - maybe_start_bgworkers
  - [TerminateChildren](../T/TerminateChildren.md)
  - [RecheckDataDirLockFile](../R/RecheckDataDirLockFile.md)
  - [TouchSocketFiles](../T/TouchSocketFiles.md)
  - [TouchSocketLockFiles](../T/TouchSocketLockFiles.md)
- Process request handlers:
  - [process_pm_shutdown_request](../p/process_pm_shutdown_request.md)
  - [process_pm_reload_request](../p/process_pm_reload_request.md)  
  - [process_pm_child_exit](../p/process_pm_child_exit.md)
  - [process_pm_pmsignal](../p/process_pm_pmsignal.md)
- Constants used:
  - WL_LATCH_SET, WL_SOCKET_ACCEPT
  - PM_RUN, PM_RECOVERY, PM_HOT_STANDBY, PM_STARTUP
  - B_CHECKPOINTER, B_BG_WRITER, B_WAL_WRITER, B_AUTOVAC_LAUNCHER, B_ARCHIVER
- Called from:
  - [PostmasterMain](../P/PostmasterMain.md)

## Notes and Other Information
- The function runs in an infinite loop until the postmaster shuts down
- High-priority requests (shutdown, reload) are processed unconditionally, even without latch events
- Socket acceptance creates ClientSocket structures that are cleaned up after backend startup
- Process restart logic varies by process type and server state (PM_RUN vs PM_RECOVERY etc.)
- Lock file validation prevents multiple postmasters from running on the same data directory
- Socket file touching prevents aggressive /tmp cleaners from removing active Unix sockets
- The loop includes PostgreSQL threading assertions when compiled with appropriate flags
- SIGKILL enforcement provides a last resort for unresponsive backends during shutdown