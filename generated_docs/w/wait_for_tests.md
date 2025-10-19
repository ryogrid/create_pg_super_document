# wait_for_tests

## Location
[src/test/regress/pg_regress.c:1548-1614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L1548-L1614)

## Overview
Waits for multiple child test processes to complete, collecting their exit statuses and stop times in a cross-platform manner for PostgreSQL regression testing.

## Definition

```c
static void
wait_for_tests(PID_TYPE * pids, int *statuses, instr_time *stoptimes,
			   char **names, int num_tests)
```
## Detailed Description
This function implements cross-platform process synchronization for PostgreSQL's parallel regression testing framework. It waits for multiple child processes (test runners) to complete, collecting their exit statuses and recording completion times. The implementation differs significantly between Unix/Linux and Windows platforms: on Unix systems it uses the wait() system call, while on Windows it uses WaitForMultipleObjects() for efficient multi-process waiting. The function tracks which processes have completed, updates the provided arrays with results, and optionally prints process names as they finish.

## Parameters / Member Variables
- `*pids`: Array of process IDs to wait for (modified during execution)
- `*statuses`: Output array to store exit statuses of completed processes
- `*stoptimes`: Output array to store completion timestamps using instr_time
- `**names`: Optional array of process names for progress reporting (can be NULL)
- `num_tests`: Number of processes to wait for
## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md) (memory allocation)
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

## Simplified Source

```c
static void wait_for_tests(PID_TYPE *pids, int *statuses, instr_time *stoptimes,
                          char **names, int num_tests) {
    int tests_left = num_tests;

#ifdef WIN32
    // Windows: Create array for WaitForMultipleObjects
    PID_TYPE *active_pids = pg_malloc(num_tests * sizeof(PID_TYPE));
    memcpy(active_pids, pids, num_tests * sizeof(PID_TYPE));
#endif

    // Wait for processes to complete
    while (tests_left > 0) {
        PID_TYPE completed_pid;

#ifndef WIN32
        // Unix: Wait for any child process
        int exit_status;
        completed_pid = wait(&exit_status);
        if (completed_pid == INVALID_PID)
            bail("failed to wait for subprocesses: %m");
#else
        // Windows: Wait for multiple objects
        DWORD exit_status;
        int result = WaitForMultipleObjects(tests_left, active_pids, FALSE, INFINITE);
        if (result < WAIT_OBJECT_0 || result >= WAIT_OBJECT_0 + tests_left)
            bail("failed to wait for subprocesses: error code %lu", GetLastError());

        completed_pid = active_pids[result - WAIT_OBJECT_0];
        // Compact the active_pids array
        active_pids[result - WAIT_OBJECT_0] = active_pids[tests_left - 1];
#endif

        // Find and update the completed process
        for (int i = 0; i < num_tests; i++) {
            if (completed_pid == pids[i]) {
#ifdef WIN32
                GetExitCodeProcess(pids[i], &exit_status);
                CloseHandle(pids[i]);
#endif
                // Mark process as completed and record results
                pids[i] = INVALID_PID;
                statuses[i] = (int) exit_status;
                INSTR_TIME_SET_CURRENT(stoptimes[i]);

                // Optional progress reporting
                if (names)
                    note_detail(" %s", names[i]);

                tests_left--;
                break;
            }
        }
    }

#ifdef WIN32
    free(active_pids);
#endif
}
```