# WaitForTerminatingWorkers

## Location
[src/bin/pg_dump/parallel.c:446-544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L446-L544)

## Overview
Waits for all worker processes in a parallel pg_dump operation to terminate completely, performing platform-specific cleanup of process handles and updating internal state.

## Definition
```c
static void WaitForTerminatingWorkers(ParallelState *pstate)
```

## Detailed Description
WaitForTerminatingWorkers is a synchronization function that ensures all worker processes have properly terminated before the main process continues. It implements a polling loop that continues until HasEveryWorkerTerminated() returns true. The function handles platform-specific waiting mechanisms and performs necessary cleanup:

The implementation uses different approaches for Unix and Windows:
- **Unix systems**: Uses the wait() system call to wait for child processes to terminate, then locates the corresponding slot and clears the PID field
- **Windows systems**: Uses WaitForMultipleObjects() to wait for thread handles, then closes the handles and clears the hThread field

After a worker is detected as terminated on either platform, the function updates the worker's status to WRKR_TERMINATED and clears the corresponding entry in the task entry array.

## Parameters / Member Variables
- `pstate`: Pointer to ParallelState structure containing information about all worker processes, including their status, PIDs/thread handles, and task entries

## Dependencies
- Functions called/Symbols referenced:
  - [HasEveryWorkerTerminated](../H/HasEveryWorkerTerminated.md) (checks if all workers have finished)
  - wait (Unix: waits for child process termination)
  - WaitForMultipleObjects (Windows: waits for multiple thread handles)
  - CloseHandle (Windows: closes thread handles)
  - [pg_malloc](../p/pg_malloc.md) (allocates memory for handle array)
  - WORKER_IS_RUNNING (macro to check worker status)
- Called from (representative examples):
  - [write_stderr](../w/write_stderr.md) (error handling context)
  - [ShutdownWorkersHard](../S/ShutdownWorkersHard.md) (forced shutdown scenario)
  - [ParallelBackupEnd](../P/ParallelBackupEnd.md) (normal cleanup at end of parallel backup)

## Notes and Other Information
- This is a static function, only used within the parallel.c module
- Function is used in both normal operation cleanup and error recovery scenarios
- Platform-specific implementations handle the differences between Unix process management and Windows threading
- Windows version requires dynamic allocation of handle array and proper cleanup of thread handles
- The function ensures proper state synchronization by updating both workerStatus and task entry arrays
- Uses infinite timeout on Windows (INFINITE parameter) to ensure all workers are properly waited for
- Located in src/bin/pg_dump/parallel.c:446-544

## Simplified Source

```c
static void
WaitForTerminatingWorkers(ParallelState *pstate)
{
    // Wait until all workers have terminated
    while (!HasEveryWorkerTerminated(pstate)) {
        ParallelSlot *slot = NULL;
        int j;

#ifndef WIN32
        // Unix: Wait for next worker process to end
        int status;
        pid_t pid = wait(&status);

        // Find the slot for the terminated worker and clear PID
        for (j = 0; j < pstate->numWorkers; j++) {
            slot = &(pstate->parallelSlot[j]);
            if (slot->pid == pid) {
                slot->pid = 0;
                break;
            }
        }
#else
        // Windows: Use WaitForMultipleObjects for thread handles
        HANDLE *lpHandles = pg_malloc(sizeof(HANDLE) * pstate->numWorkers);
        int nrun = 0;
        DWORD ret;
        uintptr_t hThread;

        // Collect running worker thread handles
        for (j = 0; j < pstate->numWorkers; j++) {
            if (WORKER_IS_RUNNING(pstate->parallelSlot[j].workerStatus)) {
                lpHandles[nrun] = (HANDLE) pstate->parallelSlot[j].hThread;
                nrun++;
            }
        }

        // Wait for any worker to terminate
        ret = WaitForMultipleObjects(nrun, lpHandles, false, INFINITE);
        hThread = (uintptr_t) lpHandles[ret - WAIT_OBJECT_0];
        free(lpHandles);

        // Find terminated worker slot and clean up thread handle
        for (j = 0; j < pstate->numWorkers; j++) {
            slot = &(pstate->parallelSlot[j]);
            if (slot->hThread == hThread) {
                CloseHandle((HANDLE) slot->hThread);
                slot->hThread = (uintptr_t) INVALID_HANDLE_VALUE;
                break;
            }
        }
#endif

        // Update worker status and clear task entry
        slot->workerStatus = WRKR_TERMINATED;
        pstate->te[j] = NULL;
    }
}
```