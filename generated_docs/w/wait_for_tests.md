# wait_for_tests

## Location
src/test/regress/pg_regress.c: 1548 - 1614

## Overview
Waits for multiple child test processes to complete, collecting their exit statuses and stop times in a cross-platform manner for PostgreSQL regression testing.

## Definition


## Detailed Description
This function implements cross-platform process synchronization for PostgreSQL's parallel regression testing framework. It waits for multiple child processes (test runners) to complete, collecting their exit statuses and recording completion times. The implementation differs significantly between Unix/Linux and Windows platforms: on Unix systems it uses the wait() system call, while on Windows it uses WaitForMultipleObjects() for efficient multi-process waiting. The function tracks which processes have completed, updates the provided arrays with results, and optionally prints process names as they finish.

## Parameters / Member Variables
- : Array of process IDs to wait for (modified during execution)
- : Output array to store exit statuses of completed processes
- : Output array to store completion timestamps using instr_time
- : Optional array of process names for progress reporting (can be NULL)
- : Number of processes to wait for

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc (memory allocation)
  - wait, WaitForMultipleObjects (platform-specific process waiting)
  - GetExitCodeProcess, CloseHandle (Windows process management)
  - INSTR_TIME_SET_CURRENT (timestamp recording)
  - note_detail (progress reporting)
  - bail (error handling)
  - PID_TYPE, INVALID_PID (platform-specific process types)
- Called from (representative examples):
  - Used in MAX_PARALLEL_TESTS context (multiple calls in src/test/regress/pg_regress.c: lines 1732, 1750, 1758, 1771)
  - [run_single_test](../r/run_single_test.md) (src/test/regress/pg_regress.c:1861)

## Notes and Other Information
- Implements platform-specific logic for Unix/Linux vs Windows process waiting
- Modifies the pids array by setting completed processes to INVALID_PID
- On Windows, maintains a separate active_pids array for WaitForMultipleObjects()
- Properly closes Windows process handles to prevent resource leaks
- Records precise completion times for performance analysis
- Supports optional progress reporting by printing process names as they complete
- Critical component for parallel test execution in PostgreSQL's regression testing framework
- Handles process synchronization errors by calling bail() to terminate testing