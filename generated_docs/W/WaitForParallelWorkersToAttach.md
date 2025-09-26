# WaitForParallelWorkersToAttach

## Location
[src/backend/access/transam/parallel.c:689-791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L689-L791)

## Overview
Waits for all parallel workers to attach to their error message queues and validates they have initialized successfully.

## Definition
```c
void WaitForParallelWorkersToAttach(ParallelContext *pcxt)
```

## Detailed Description
This function ensures that all launched parallel workers have successfully attached to their error message queues, which indicates they have properly initialized. The function provides reliability guarantees for parallel operations by:

1. Checking each worker's attachment status through their error message queues
2. Handling various worker states (not started, started, stopped)
3. Throwing errors if workers fail to initialize properly
4. Using latches to efficiently wait for state changes

The function is optional but recommended when the leader process needs to ensure all workers are ready before proceeding. It provides early detection of worker startup failures, though such failures are rare. Workers that fail to attach are detected and reported as initialization errors.

## Parameters / Member Variables
- `pcxt`: Pointer to the ParallelContext containing worker information and state

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - [GetBackgroundWorkerPid](../G/GetBackgroundWorkerPid.md)
  - [shm_mq_get_queue](../s/shm_mq_get_queue.md)
  - [shm_mq_get_sender](../s/shm_mq_get_sender.md)
  - [WaitLatch](WaitLatch.md)
  - [ResetLatch](../R/ResetLatch.md)
  - ereport
- Called from (representative examples):
  - [_brin_begin_parallel](../b/_brin_begin_parallel.md)
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md)

## Notes and Other Information
- Skip execution if no workers were launched (nworkers_launched == 0)
- Updates known_attached_workers array to track worker attachment status
- Uses WaitLatch with WL_LATCH_SET | WL_EXIT_ON_PM_DEATH for efficient waiting
- Critical for ensuring parallel operation reliability but not always necessary
- Early startup failures are uncommon, so leaders should do useful work before calling this function