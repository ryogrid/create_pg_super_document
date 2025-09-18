# ParallelQueryMain

## Location
src/backend/executor/execParallel.c: 1400 - 1503

## Overview
Main entrypoint function for parallel query worker processes that executes the serialized query plan and writes results to the appropriate tuple queue.

## Definition
```c
void ParallelQueryMain(dsm_segment *seg, shm_toc *toc)
```

## Detailed Description
This function serves as the primary execution entry point for parallel worker processes in PostgreSQL's parallel query execution system. It coordinates the entire lifecycle of parallel query execution within a worker process, from initialization through cleanup. The function retrieves a serialized PlannedStmt from shared memory, sets up the execution environment, runs the query plan, and reports results and instrumentation data back to the leader.

The function handles several critical aspects of parallel execution including: setting up tuple destinations, configuring instrumentation, initializing the executor with worker-specific context, executing the plan with proper tuple bounds, and collecting performance statistics. It ensures proper resource management and cleanup while coordinating with the parallel leader through shared memory structures.

## Parameters / Member Variables
- `seg`: Dynamic shared memory segment containing all shared parallel execution data
- `toc`: Shared memory table of contents for locating specific data structures within the segment

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_lookup
  - ExecParallelGetReceiver
  - ExecParallelGetQueryDesc
  - pgstat_report_activity
  - dsa_attach_in_place
  - ExecutorStart
  - ExecutorRun
  - ExecutorFinish
  - ExecutorEnd
  - dsa_get_address
  - RestoreParamExecParams
  - ExecParallelInitializeWorker
  - ExecSetTupleBound
  - InstrStartParallelQuery
  - InstrEndParallelQuery
  - ExecParallelReportInstrumentation
  - dsa_detach
  - FreeQueryDesc
  - DsaPointerIsValid
- Constants used:
  - PARALLEL_KEY_EXECUTOR_FIXED
  - PARALLEL_KEY_INSTRUMENTATION
  - PARALLEL_KEY_JIT_INSTRUMENTATION
  - PARALLEL_KEY_DSA
  - PARALLEL_KEY_BUFFER_USAGE
  - PARALLEL_KEY_WAL_USAGE
  - STATE_RUNNING
  - ForwardScanDirection
- Global variables:
  - debug_query_string
  - ParallelWorkerNumber
- Types used:
  - FixedParallelExecutorState
  - BufferUsage
  - WalUsage
  - DestReceiver
  - QueryDesc
  - SharedExecutorInstrumentation
  - SharedJitInstrumentation
  - dsa_area
  - ParallelWorkerContext

## Notes and Other Information
- This function is called from ParallelWorkerMain after basic parallel environment setup is complete
- Assumes transaction state, combo CID mappings, and GUC values are already properly configured
- Handles both regular execution statistics and JIT compilation instrumentation
- Properly manages dynamic shared memory area attachment and detachment
- Sets debug_query_string for individual workers to support monitoring and debugging
- Respects tuple bounds passed from the leader to limit result set size
- Ensures proper cleanup of all resources including query descriptors and receivers
- Critical component that enables PostgreSQL to distribute query execution across multiple worker processes
- Buffer and WAL usage tracking starts after executor initialization to match leader behavior