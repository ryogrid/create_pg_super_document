# ParallelWorkerShutdown

## Location
[src/backend/access/transam/parallel.c:1601-1628](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L1601-L1628)

## Overview
ParallelWorkerShutdown is a cleanup function registered as a before_shmem_exit hook that ensures proper communication with the parallel leader and detachment from shared memory segments when a parallel worker exits.

## Definition
```c
static void ParallelWorkerShutdown(int code, Datum arg)
```

## Detailed Description
ParallelWorkerShutdown serves as a critical cleanup function that is automatically invoked when a parallel worker process is about to exit. The function addresses two important aspects of parallel worker termination:

1. **Leader notification**: Sends a signal to the parallel leader process to ensure the leader attempts to read from the worker's error queue one more time. This is crucial for handling cases where the worker exits uncleanly without sending a proper ErrorResponse, such as when `proc_exit` is called directly.

2. **Shared memory cleanup**: Explicitly detaches from the DSM (Dynamic Shared Memory) segment to allow subsystems using `on_dsm_detach()` hooks to execute cleanup code and send statistics before the stats subsystem is shut down by other `before_shmem_exit()` hooks.

The function guards against race conditions where worker statistics or other important data might be lost due to improper shutdown ordering. The explicit DSM detachment is necessary because relying on careful DSM attachment ordering doesn't work reliably due to potential hash table growth causing new segment allocations.

## Parameters / Member Variables
- `code`: The exit code being passed to the shutdown process (unused in this function)
- `arg`: A Datum containing a pointer to the dsm_segment that should be detached

## Dependencies
- Functions called/Symbols referenced:
  - [SendProcSignal](../S/SendProcSignal.md)
  - PROCSIG_PARALLEL_MESSAGE (signal type constant)
  - [dsm_detach](../d/dsm_detach.md)
  - dsm_segment (struct type)
  - [DatumGetPointer](../D/DatumGetPointer.md) (macro for extracting pointer from Datum)
- Called from (representative examples):
  - [ParallelWorkerMain](ParallelWorkerMain.md) (registered via before_shmem_exit)

## Notes and Other Information
- Declared as `static` - only visible within parallel.c
- Registered as a `before_shmem_exit` callback during parallel worker initialization
- Uses global variables `ParallelLeaderPid` and `ParallelLeaderProcNumber` to identify the leader
- Critical for preventing loss of error messages and statistics during worker shutdown
- The signal sent to the leader is `PROCSIG_PARALLEL_MESSAGE` which prompts error queue processing
- Explicit DSM detachment is required due to potential issues with automatic cleanup ordering