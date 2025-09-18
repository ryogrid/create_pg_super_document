# ExecParallelCleanup

## Location
[src/backend/executor/execParallel.c:1184-1219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L1184-L1219)

## Overview
Performs final cleanup of parallel execution resources by retrieving instrumentation data from shared memory and deallocating all remaining parallel execution structures.

## Definition
```c
void ExecParallelCleanup(ParallelExecutorInfo *pei)
```

## Detailed Description
ExecParallelCleanup completes the parallel execution lifecycle by performing final instrumentation collection and comprehensive resource cleanup. The function is designed to be called after ExecParallelFinish, allowing for potential examination of Dynamic Shared Memory (DSM) contents between the two calls.

The cleanup process follows this sequence:

1. **Retrieves instrumentation data**: If instrumentation was enabled, calls ExecParallelRetrieveInstrumentation to collect performance statistics from all workers and aggregate them into local structures

2. **Retrieves JIT instrumentation**: If JIT compilation instrumentation exists, calls ExecParallelRetrieveJitInstrumentation to gather JIT-specific performance data from workers

3. **Frees serialized parameters**: If dynamic shared area (DSA) pointers for serialized execution parameters exist, frees them using dsa_free and marks them invalid

4. **Detaches from shared memory area**: Detaches from the DSA using dsa_detach, releasing the connection to shared memory structures

5. **Destroys parallel context**: Calls DestroyParallelContext to clean up the core parallel execution infrastructure, including worker processes and shared memory segments

6. **Frees the executor info**: Finally deallocates the ParallelExecutorInfo structure itself

## Parameters / Member Variables
- `pei`: The ParallelExecutorInfo structure containing all parallel execution state, instrumentation data, shared memory areas, and parallel context information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecParallelRetrieveInstrumentation](ExecParallelRetrieveInstrumentation.md)
  - [ExecParallelRetrieveJitInstrumentation](ExecParallelRetrieveJitInstrumentation.md)
  - DsaPointerIsValid
  - [dsa_free](../d/dsa_free.md)
  - InvalidDsaPointer
  - dsa_detach
  - DestroyParallelContext
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [ExecShutdownGather](ExecShutdownGather.md)
  - [ExecShutdownGatherMerge](ExecShutdownGatherMerge.md)

## Notes and Other Information
- This is a public function that completes the parallel execution cleanup process
- Must be called after ExecParallelFinish to ensure workers have completed before final resource cleanup
- The separation from ExecParallelFinish allows callers to examine DSM contents between worker completion and memory deallocation
- Handles instrumentation retrieval for both regular execution statistics and JIT compilation metrics
- Provides comprehensive cleanup of all parallel execution resources including shared memory, dynamic shared areas, and worker contexts
- Critical for preventing memory leaks and ensuring proper resource management in parallel query execution
- The final pfree(pei) invalidates the ParallelExecutorInfo pointer, so it should not be used after this call