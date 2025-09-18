# die

## Location
src/backend/tcop/postgres.c: 2999 - 3028

## Overview
Signal handler function that schedules graceful process termination when a shutdown signal is received from the postmaster.

## Definition
```c
void die(SIGNAL_ARGS)
```

## Detailed Description
This signal handler function manages graceful shutdown of PostgreSQL backend processes when they receive a termination signal (typically SIGTERM) from the postmaster. Unlike `quickdie` which performs immediate termination, `die` schedules a graceful shutdown that allows the current transaction to complete properly.

The function performs several key operations:
1. Sets interrupt and process death pending flags to signal that termination is requested
2. Records the session end cause for statistical purposes
3. Wakes up any processes waiting on the process latch
4. Handles special case for single-user mode where immediate processing is needed

The graceful approach allows for proper transaction cleanup and resource deallocation, unlike the emergency termination performed by `quickdie`. The actual termination logic is deferred to the normal interrupt processing mechanism.

## Parameters / Member Variables
- `SIGNAL_ARGS`: Standard signal handler arguments (signal number, signal info, context)

## Dependencies
- Functions called/Symbols referenced:
  - SIGNAL_ARGS (signal handler argument macro)
  - InterruptPending (global flag for pending interrupts)
  - ProcDiePending (global flag for pending process death)
  - proc_exit_inprogress (global flag indicating exit in progress)
  - DISCONNECT_KILLED (session end cause constant)
  - pgStatSessionEndCause (global variable for statistics)
  - [SetLatch](../S/SetLatch.md) (function to wake waiting processes)
  - MyLatch (current process latch)
  - DoingCommandRead (global flag for command reading state)
  - DestRemote (output destination constant)
  - whereToSendOutput (global variable for output destination)
  - ProcessInterrupts (function to handle pending interrupts)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (in src/backend/tcop/postgres.c:4274, 4287) - registered as signal handler
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md), AutoVacWorkerMain, ApplyLauncherMain (various worker processes)
  - Multiple test utilities in src/bin/pg_test_fsync/

## Notes and Other Information
- Provides graceful shutdown mechanism compared to immediate termination in `quickdie`
- Uses interrupt pending flags to integrate with PostgreSQL's normal interrupt processing\n- Records session termination cause for monitoring and statistics\n- Handles special case for single-user mode where latches are not available\n- Widely used across PostgreSQL processes including parallel workers, autovacuum, and replication workers\n- The function name \"die\" reflects its role in process termination, but in a controlled manner\n- Critical component of PostgreSQL's process lifecycle and shutdown management