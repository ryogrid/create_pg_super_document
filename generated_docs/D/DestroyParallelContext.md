# DestroyParallelContext

## Location
src/backend/access/transam/parallel.c: 946 - 1019

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
  - dlist_delete
  - TerminateBackgroundWorker
  - shm_mq_detach
  - dsm_detach
  - HOLD_INTERRUPTS/RESUME_INTERRUPTS
  - WaitForParallelWorkersToExit
  - pfree
- Called from (representative examples):
  - _brin_end_parallel
  - _bt_end_parallel
  - ExecParallelCleanup
  - AtEOXact_Parallel
  - parallel_vacuum_end

## Notes and Other Information
- Safe to call even when WaitForParallelWorkersToFinish hasn't been called first
- Handles both shared memory and private memory contexts
- Critical for transaction cleanup (called from AtEOXact_Parallel and AtEOSubXact_Parallel)
- Uses uninterruptible section during final worker cleanup to ensure transaction consistency
- Order of operations is crucial to prevent double-cleanup during error scenarios