# ExecParallelRetrieveJitInstrumentation

## Location
[src/backend/executor/execParallel.c:1091-1130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L1091-L1130)

## Overview
Aggregates Just-In-Time (JIT) compilation instrumentation data from all parallel workers and stores both combined statistics and per-worker details.

## Definition
```c
static void ExecParallelRetrieveJitInstrumentation(PlanState *planstate, SharedJitInstrumentation *shared_jit)
```

## Detailed Description
ExecParallelRetrieveJitInstrumentation consolidates JIT compilation performance data from parallel workers after query execution completes. The function performs two primary operations:

1. **Aggregates worker JIT statistics**: Creates or uses the existing combined JIT instrumentation structure (es_jit_worker_instr) and accumulates statistics from all workers using InstrJitAgg. This provides an overall view of JIT compilation performance across the entire parallel query.

2. **Preserves per-worker JIT details**: Allocates memory in the per-query context and copies the complete SharedJitInstrumentation structure, including individual worker JIT data. This allows for detailed analysis of JIT performance variations across different workers.

The function ensures that JIT instrumentation data survives beyond the parallel execution phase and is available for query performance analysis and reporting.

## Parameters / Member Variables
- `planstate`: The PlanState node associated with the query execution, used to access the executor state and store instrumentation data
- `shared_jit`: The SharedJitInstrumentation structure containing JIT performance data from all parallel workers

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [InstrJitAgg](../I/InstrJitAgg.md)  
  - [mul_size](../m/mul_size.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - memcpy
- Called from (representative examples):
  - [ExecParallelCleanup](ExecParallelCleanup.md)

## Notes and Other Information
- This is a static function internal to execParallel.c
- Memory allocation occurs in the per-query memory context (es_query_cxt) to ensure proper lifetime management
- The function only executes when JIT compilation is enabled and parallel workers have generated JIT instrumentation data
- Provides essential data for understanding JIT compilation overhead and effectiveness in parallel queries
- Works in conjunction with the regular instrumentation retrieval to provide comprehensive performance analysis
- The combined instrumentation (es_jit_worker_instr) is lazily allocated only when needed

## Simplified Source

```c
static void ExecParallelRetrieveJitInstrumentation(PlanState *planstate,
                                                  SharedJitInstrumentation *shared_jit) {
    JitInstrumentation *combined;
    int ibytes;
    int n;

    // Allocate combined JIT instrumentation if not already present
    if (!planstate->state->es_jit_worker_instr) {
        planstate->state->es_jit_worker_instr =
            MemoryContextAllocZero(planstate->state->es_query_cxt, sizeof(JitInstrumentation));
    }
    combined = planstate->state->es_jit_worker_instr;

    // Aggregate JIT statistics from all workers
    for (n = 0; n < shared_jit->num_workers; ++n) {
        InstrJitAgg(combined, &shared_jit->jit_instr[n]);
    }

    // Store per-worker JIT detail in query memory context
    ibytes = offsetof(SharedJitInstrumentation, jit_instr) +
             mul_size(shared_jit->num_workers, sizeof(JitInstrumentation));
    planstate->worker_jit_instrument =
        MemoryContextAlloc(planstate->state->es_query_cxt, ibytes);

    // Copy all worker JIT instrumentation data
    memcpy(planstate->worker_jit_instrument, shared_jit, ibytes);
}
```