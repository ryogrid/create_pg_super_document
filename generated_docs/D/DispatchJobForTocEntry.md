# DispatchJobForTocEntry

## Location
[src/bin/pg_dump/parallel.c:1205-1235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1205-L1235)

## Overview
Dispatches a database object processing job to an available worker process in pg_dump parallel operations, managing worker assignment and job tracking.

## Definition
```c
void DispatchJobForTocEntry(ArchiveHandle *AH, ParallelState *pstate, TocEntry *te, T_Action act, ParallelCompletionPtr callback, void *callback_data)
```

## Detailed Description
This function is the core job dispatcher for parallel pg_dump operations. It assigns database object processing tasks to available worker processes by first finding an idle worker (waiting if necessary), constructing and sending the appropriate command, and updating the worker state to track the assigned job. The function supports different types of actions (dump, restore, etc.) and maintains callback information for handling job completion. If no workers are immediately available, it will block until a worker becomes idle, potentially triggering previously registered callback functions during the wait.

## Parameters / Member Variables
- `AH`: Pointer to the main ArchiveHandle structure managing the dump/restore operation
- `pstate`: Pointer to the ParallelState structure that tracks all worker processes and their states
- `te`: Pointer to the TocEntry representing the specific database object to be processed
- `act`: The action type (T_Action) specifying what operation to perform on the object
- `callback`: Function pointer to call when the job completes
- `callback_data`: User-defined data to pass to the completion callback function

## Dependencies
- Functions called/Symbols referenced:
  - [GetIdleWorker](../G/GetIdleWorker.md)
  - [WaitForWorkers](../W/WaitForWorkers.md)
  - [buildWorkerCommand](../b/buildWorkerCommand.md)
  - [sendMessageToWorker](../s/sendMessageToWorker.md)
  - [ParallelState](../P/ParallelState.md) (struct)
  - [TocEntry](../T/TocEntry.md) (struct)
  - T_Action (enum)
  - NO_SLOT (constant)
  - WFW_ONE_IDLE (constant)
  - WRKR_WORKING (constant)
- Called from (representative examples):
  - [WriteDataChunks](../W/WriteDataChunks.md)
  - [restore_toc_entries_parallel](../r/restore_toc_entries_parallel.md)

## Notes and Other Information
- This function may block if no workers are available, making it synchronous from the caller's perspective
- Worker state is carefully managed to track which TocEntry each worker is processing
- The callback mechanism allows for flexible handling of job completion events
- Part of the parallel infrastructure that enables pg_dump and pg_restore to utilize multiple processes for improved performance
- Commands are constructed dynamically based on the action type and TocEntry properties

## Simplified Source

```c
void
DispatchJobForTocEntry(ArchiveHandle *AH,
                      ParallelState *pstate,
                      TocEntry *te,
                      T_Action act,
                      ParallelCompletionPtr callback,
                      void *callback_data)
{
    int worker;
    char buf[256];

    // Find an idle worker, waiting if necessary
    while ((worker = GetIdleWorker(pstate)) == NO_SLOT)
        WaitForWorkers(AH, pstate, WFW_ONE_IDLE);

    // Build and send command to worker
    buildWorkerCommand(AH, te, act, buf, sizeof(buf));
    sendMessageToWorker(pstate, worker, buf);

    // Update worker state and tracking information
    pstate->parallelSlot[worker].workerStatus = WRKR_WORKING;
    pstate->parallelSlot[worker].callback = callback;
    pstate->parallelSlot[worker].callback_data = callback_data;
    pstate->te[worker] = te;
}
```