# consoleHandler

## Location
[src/fe_utils/cancel.c:195-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/cancel.c#L195-L231)

## Overview
consoleHandler is a Windows-specific console interrupt handler that manages graceful shutdown of pg_dump parallel operations when Ctrl+C or Ctrl+Break is pressed.

## Definition

```c
static BOOL WINAPI
consoleHandler(DWORD dwCtrlType)
```
## Detailed Description
consoleHandler is a Windows console control handler function that responds to console interrupt events (CTRL_C_EVENT and CTRL_BREAK_EVENT) in pg_dump's parallel processing mode. When triggered, it performs an orderly shutdown by first terminating worker threads, sending query cancellation requests to all connected database backends, and then allowing the default process termination to proceed.

The function operates within a critical section to ensure thread-safe access to shared data structures. It handles both worker threads (which are forcibly terminated using TerminateThread) and the leader connection, sending PQcancel requests to all active database connections to cleanly abort any running queries before the process exits.

## Parameters / Member Variables
- `dwCtrlType`: Windows control event type (CTRL_C_EVENT, CTRL_BREAK_EVENT, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - [PQcancel](../P/PQcancel.md)
  - [write_stderr](../w/write_stderr.md)
  - [ParallelSlot](../P/ParallelSlot.md) (type)
- Called from (representative examples):
  - [set_cancel_handler](../s/set_cancel_handler.md) (pg_dump)
  - [setup_cancel_handler](../s/setup_cancel_handler.md) (fe_utils)

## Notes and Other Information
- Windows-specific function (WIN32 only) using WINAPI calling convention
- Uses TerminateThread which may leak resources, but this is acceptable since the entire process is terminating
- Protected by critical section (signal_info_lock) to prevent race conditions with other threads
- Handles both parallel worker threads and the main leader connection
- Always returns FALSE to allow Windows default signal handling to continue (which will exit the process)
- Uses simple write_stderr for output since other threads have been terminated uncleanly
- Part of pg_dump's parallel backup functionality, ensuring clean cancellation of database operations

## Simplified Source

```c
static BOOL WINAPI
consoleHandler(DWORD dwCtrlType)
{
    int i;
    char errbuf[1];

    // Handle Ctrl+C and Ctrl+Break events
    if (dwCtrlType == CTRL_C_EVENT || dwCtrlType == CTRL_BREAK_EVENT) {
        // Enter critical section for thread-safe access
        EnterCriticalSection(&signal_info_lock);

        // Stop worker threads and cancel their database connections
        if (signal_info.pstate != NULL) {
            for (i = 0; i < signal_info.pstate->numWorkers; i++) {
                ParallelSlot *slot = &(signal_info.pstate->parallelSlot[i]);
                ArchiveHandle *AH = slot->AH;
                HANDLE hThread = (HANDLE) slot->hThread;

                // Terminate worker thread (resource leaks acceptable since process is ending)
                if (hThread != INVALID_HANDLE_VALUE)
                    TerminateThread(hThread, 0);

                // Cancel database query for this worker
                if (AH != NULL && AH->connCancel != NULL)
                    PQcancel(AH->connCancel, errbuf, sizeof(errbuf));
            }
        }

        // Cancel leader connection query
        if (signal_info.myAH != NULL && signal_info.myAH->connCancel != NULL)
            PQcancel(signal_info.myAH->connCancel, errbuf, sizeof(errbuf));

        LeaveCriticalSection(&signal_info_lock);

        // Report termination to user
        if (progname) {
            write_stderr(progname);
            write_stderr(": ");
        }
        write_stderr("terminated by user\n");
    }

    // Return FALSE to allow default Windows signal handling (process termination)
    return FALSE;
}
```