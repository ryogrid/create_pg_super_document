# process_pm_child_exit

## Location
[src/backend/postmaster/postmaster.c:2354-2695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L2354-L2695)

## Overview
Handles cleanup and state transitions when child processes exit, managing different types of PostgreSQL processes and coordinating proper shutdown or restart procedures.

## Definition
```c
static void process_pm_child_exit(void)
```

## Detailed Description
This function is responsible for processing child process exits in the PostgreSQL postmaster. It uses `waitpid()` with WNOHANG to collect information about terminated child processes and handles different types of processes appropriately:

- **Startup Process**: Critical for database initialization and recovery
- **Background Writer**: Handles dirty buffer writes  
- **Checkpointer**: Manages checkpoint operations
- **WAL Writer**: Writes WAL buffers to disk
- **WAL Receiver**: Receives WAL from primary in standby mode
- **WAL Summarizer**: Creates WAL summaries
- **Autovacuum Launcher**: Coordinates automatic vacuuming
- **Archiver**: Archives completed WAL files
- **System Logger**: Handles logging infrastructure
- **Slot Sync Worker**: Synchronizes replication slots
- **Background Workers**: Custom background processes
- **Backend Processes**: Client connection handlers

The function determines whether to restart processes, crash the system, or transition to shutdown states based on the process type and exit status.

## Parameters / Member Variables
This function operates on global state variables including:
- `pending_pm_child_exit`: Flag indicating child exit pending
- Various process ID variables (StartupPID, BgWriterPID, etc.)
- `pmState`: Current postmaster state
- `Shutdown`: Current shutdown mode

## Dependencies
- Functions called/Symbols referenced:
  - [waitpid](../w/waitpid.md) - Collects child process exit information
  - [HandleChildCrash](../H/HandleChildCrash.md) - Handles abnormal process exits
  - [StartChildProcess](../S/StartChildProcess.md) - Restarts terminated processes
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md) - Advances postmaster state
  - [CleanupBackgroundWorker](../C/CleanupBackgroundWorker.md) - Handles background worker cleanup
  - [CleanupBackend](../C/CleanupBackend.md) - Handles backend process cleanup
  - [TerminateChildren](../T/TerminateChildren.md) - Sends termination signals
  - [ExitPostmaster](../E/ExitPostmaster.md) - Exits postmaster process
- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md) - Main postmaster event loop

## Notes and Other Information
- Uses a loop with `waitpid(-1, &exitstatus, WNOHANG)` to collect all pending child exit notifications
- Different process types have different restart policies and crash handling
- The startup process is treated specially as its failure can be catastrophic
- Normal exits (status 0) and FATAL exits (status 1) are generally handled gracefully
- The function coordinates with the postmaster state machine to manage system-wide state transitions
- Background workers and regular backends have their own specialized cleanup functions
- System logger is restarted immediately for safety when it exits
- Process restart decisions depend on current postmaster state and shutdown mode