# WaitForParallelWorkersToFinish

## Location
[src/backend/access/transam/parallel.c:792-905](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L792-L905)

## Overview
Waits for all parallel workers to complete their computations and handles any errors they may have encountered during execution or shutdown.

## Definition
```c
void WaitForParallelWorkersToFinish(ParallelContext *pcxt)
```

## Detailed Description
This function ensures all parallel workers have finished their assigned work and collects any error messages they may have generated. It serves several critical purposes:

1. **Error Collection**: Processes pending parallel messages and propagates any worker errors to the leader
2. **Completion Verification**: Ensures all workers have cleanly finished their computations
3. **Transaction Coordination**: Updates XactLastRecEnd based on worker feedback for transaction consistency
4. **Failure Detection**: Identifies workers that failed to initialize or terminated abnormally

The function continuously polls worker status until all are confirmed finished, handling cases where workers may not have started yet or failed during startup. It uses latches for efficient waiting and processes interrupts to handle error propagation.

## Parameters / Member Variables
- `pcxt`: Pointer to the ParallelContext containing information about all launched workers

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - [GetBackgroundWorkerPid](../G/GetBackgroundWorkerPid.md)
  - [shm_mq_get_queue](../s/shm_mq_get_queue.md)
  - [shm_mq_get_sender](../s/shm_mq_get_sender.md)
  - [WaitLatch](WaitLatch.md)
  - [ResetLatch](../R/ResetLatch.md)
  - [shm_toc_lookup](../s/shm_toc_lookup.md)
  - ereport
- Called from (representative examples):
  - [_brin_end_parallel](../b/_brin_end_parallel.md)
  - [_bt_end_parallel](../b/_bt_end_parallel.md)
  - [ExecParallelFinish](../E/ExecParallelFinish.md)
  - [parallel_vacuum_process_all_indexes](../p/parallel_vacuum_process_all_indexes.md)

## Notes and Other Information
- Essential function that must be called after parallel operations to ensure correctness
- Handles both successful completion and various failure scenarios
- Updates transaction state (XactLastRecEnd) from worker feedback when applicable
- Uses WAIT_EVENT_PARALLEL_FINISH for wait event monitoring
- Critical for maintaining transaction consistency and proper error handling in parallel operations

## Simplified Source

```c
void WaitForParallelWorkersToFinish(ParallelContext *pcxt) {
    for (;;) {
        bool anyone_alive = false;
        int nfinished = 0;

        // Process pending messages and check for worker errors
        CHECK_FOR_INTERRUPTS();

        // Check status of all launched workers
        for (int i = 0; i < pcxt->nworkers_launched; ++i) {
            if (pcxt->worker[i].error_mqh == NULL) {
                ++nfinished;  // Worker exited cleanly
            } else if (pcxt->known_attached_workers[i]) {
                anyone_alive = true;
                break;
            }
        }

        if (!anyone_alive) {
            // All workers finished successfully
            if (nfinished >= pcxt->nworkers_launched) {
                Assert(nfinished == pcxt->nworkers_launched);
                break;
            }

            // Check for workers that failed to start or terminated abnormally
            for (int i = 0; i < pcxt->nworkers_launched; ++i) {
                pid_t pid;
                if (pcxt->worker[i].error_mqh == NULL ||
                    pcxt->worker[i].bgwhandle == NULL ||
                    GetBackgroundWorkerPid(pcxt->worker[i].bgwhandle, &pid) != BGWH_STOPPED)
                    continue;

                // Check if worker failed to initialize
                shm_mq *mq = shm_mq_get_queue(pcxt->worker[i].error_mqh);
                if (shm_mq_get_sender(mq) == NULL)
                    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("parallel worker failed to initialize"),
                            errhint("More details may be available in the server log.")));
            }
        }

        // Wait for worker status changes
        WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, WAIT_EVENT_PARALLEL_FINISH);
        ResetLatch(MyLatch);
    }

    // Update transaction state from worker feedback
    if (pcxt->toc != NULL) {
        FixedParallelState *fps = shm_toc_lookup(pcxt->toc, PARALLEL_KEY_FIXED, false);
        if (fps->last_xlog_end > XactLastRecEnd)
            XactLastRecEnd = fps->last_xlog_end;
    }
}
```