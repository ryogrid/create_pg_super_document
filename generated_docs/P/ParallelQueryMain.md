# ParallelQueryMain

## Location
[src/backend/executor/execParallel.c:1400-1503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L1400-L1503)

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
  - [shm_toc_lookup](../s/shm_toc_lookup.md)
  - [ExecParallelGetReceiver](../E/ExecParallelGetReceiver.md)
  - [ExecParallelGetQueryDesc](../E/ExecParallelGetQueryDesc.md)
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
  - [dsa_attach_in_place](../d/dsa_attach_in_place.md)
  - [ExecutorStart](../E/ExecutorStart.md)
  - [ExecutorRun](../E/ExecutorRun.md)
  - [ExecutorFinish](../E/ExecutorFinish.md)
  - [ExecutorEnd](../E/ExecutorEnd.md)
  - [dsa_get_address](../d/dsa_get_address.md)
  - [RestoreParamExecParams](../R/RestoreParamExecParams.md)
  - [ExecParallelInitializeWorker](../E/ExecParallelInitializeWorker.md)
  - [ExecSetTupleBound](../E/ExecSetTupleBound.md)
  - [InstrStartParallelQuery](../I/InstrStartParallelQuery.md)
  - [InstrEndParallelQuery](../I/InstrEndParallelQuery.md)
  - [ExecParallelReportInstrumentation](../E/ExecParallelReportInstrumentation.md)
  - [dsa_detach](../d/dsa_detach.md)
  - [FreeQueryDesc](../F/FreeQueryDesc.md)
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
  - [FixedParallelExecutorState](../F/FixedParallelExecutorState.md)
  - [BufferUsage](../B/BufferUsage.md)
  - [WalUsage](../W/WalUsage.md)
  - [DestReceiver](../D/DestReceiver.md)
  - [QueryDesc](../Q/QueryDesc.md)
  - [SharedExecutorInstrumentation](../S/SharedExecutorInstrumentation.md)
  - [SharedJitInstrumentation](../S/SharedJitInstrumentation.md)
  - [dsa_area](../d/dsa_area.md)
  - [ParallelWorkerContext](ParallelWorkerContext.md)

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

## Simplified Source

```c
void ParallelQueryMain(dsm_segment *seg, shm_toc *toc)
{
    // Get fixed execution state from shared memory
    FixedParallelExecutorState *fpes = shm_toc_lookup(toc, PARALLEL_KEY_EXECUTOR_FIXED, false);

    // Set up tuple destination and instrumentation
    DestReceiver *receiver = ExecParallelGetReceiver(seg, toc);
    SharedExecutorInstrumentation *instrumentation =
        shm_toc_lookup(toc, PARALLEL_KEY_INSTRUMENTATION, true);
    int instrument_options = instrumentation ? instrumentation->instrument_options : 0;

    // Create QueryDesc with all necessary execution context
    QueryDesc *queryDesc = ExecParallelGetQueryDesc(toc, receiver, instrument_options);

    // Set up debug information and activity reporting
    debug_query_string = queryDesc->sourceText;
    pgstat_report_activity(STATE_RUNNING, debug_query_string);

    // Attach to dynamic shared memory area
    void *area_space = shm_toc_lookup(toc, PARALLEL_KEY_DSA, false);
    dsa_area *area = dsa_attach_in_place(area_space, seg);

    // Initialize executor with parallel worker context
    queryDesc->plannedstmt->jitFlags = fpes->jit_flags;
    ExecutorStart(queryDesc, fpes->eflags);
    queryDesc->planstate->state->es_query_dsa = area;

    // Restore parameter execution state if present
    if (DsaPointerIsValid(fpes->param_exec)) {
        char *paramexec_space = dsa_get_address(area, fpes->param_exec);
        RestoreParamExecParams(paramexec_space, queryDesc->estate);
    }

    // Initialize worker-specific state and set tuple bounds
    ParallelWorkerContext pwcxt = {.toc = toc, .seg = seg};
    ExecParallelInitializeWorker(queryDesc->planstate, &pwcxt);
    ExecSetTupleBound(fpes->tuples_needed, queryDesc->planstate);

    // Start performance tracking and execute the query
    InstrStartParallelQuery();
    ExecutorRun(queryDesc, ForwardScanDirection,
                fpes->tuples_needed < 0 ? 0 : fpes->tuples_needed, true);
    ExecutorFinish(queryDesc);

    // Report buffer and WAL usage statistics
    BufferUsage *buffer_usage = shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false);
    WalUsage *wal_usage = shm_toc_lookup(toc, PARALLEL_KEY_WAL_USAGE, false);
    InstrEndParallelQuery(&buffer_usage[ParallelWorkerNumber], &wal_usage[ParallelWorkerNumber]);

    // Report execution instrumentation if enabled
    if (instrumentation != NULL)
        ExecParallelReportInstrumentation(queryDesc->planstate, instrumentation);

    // Report JIT instrumentation if present
    SharedJitInstrumentation *jit_instrumentation =
        shm_toc_lookup(toc, PARALLEL_KEY_JIT_INSTRUMENTATION, true);
    if (queryDesc->estate->es_jit && jit_instrumentation != NULL) {
        jit_instrumentation->jit_instr[ParallelWorkerNumber] =
            queryDesc->estate->es_jit->instr;
    }

    // Clean up resources
    ExecutorEnd(queryDesc);
    dsa_detach(area);
    FreeQueryDesc(queryDesc);
    receiver->rDestroy(receiver);
}
```