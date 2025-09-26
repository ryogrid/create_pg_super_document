# DestroyParallelContext

## Location
[src/backend/access/transam/parallel.c:946-1019](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L946-L1019)

## Overview
Destroys a parallel context by terminating remaining workers, cleaning up shared memory, and freeing all associated resources.

## Definition
```c
void DestroyParallelContext(ParallelContext *pcxt)
```

## Detailed Description
This function performs complete cleanup of a parallel context, handling both graceful and forceful shutdown scenarios. The destruction process follows a careful sequence:

1. **Context Removal**: Immediately removes the context from the global list to prevent re-entry during error handling
2. **Worker Termination**: Forcibly terminates any remaining background workers using TerminateBackgroundWorker
3. **Queue Cleanup**: Detaches from shared message queues used for error reporting
4. **Memory Cleanup**: Detaches from dynamic shared memory segments or frees private memory
5. **Complete Shutdown**: Uses HOLD_INTERRUPTS/RESUME_INTERRUPTS around WaitForParallelWorkersToExit to ensure uninterruptible cleanup
6. **Resource Freeing**: Frees worker arrays, library/function names, and the context itself

The function is designed to be safe to call even when workers haven't finished cleanly, making it suitable for both normal completion and error recovery scenarios.

## Parameters / Member Variables
- `pcxt`: Pointer to the ParallelContext to be destroyed and cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_delete](../d/dlist_delete.md)
  - [TerminateBackgroundWorker](../T/TerminateBackgroundWorker.md)
  - [shm_mq_detach](../s/shm_mq_detach.md)
  - [dsm_detach](../d/dsm_detach.md)
  - HOLD_INTERRUPTS/RESUME_INTERRUPTS
  - [WaitForParallelWorkersToExit](../W/WaitForParallelWorkersToExit.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [_brin_end_parallel](../b/_brin_end_parallel.md)
  - [_bt_end_parallel](../b/_bt_end_parallel.md)
  - [ExecParallelCleanup](../E/ExecParallelCleanup.md)
  - [AtEOXact_Parallel](../A/AtEOXact_Parallel.md)
  - [parallel_vacuum_end](../p/parallel_vacuum_end.md)

## Notes and Other Information
- Safe to call even when WaitForParallelWorkersToFinish hasn't been called first
- Handles both shared memory and private memory contexts
- Critical for transaction cleanup (called from AtEOXact_Parallel and AtEOSubXact_Parallel)
- Uses uninterruptible section during final worker cleanup to ensure transaction consistency
- Order of operations is crucial to prevent double-cleanup during error scenarios