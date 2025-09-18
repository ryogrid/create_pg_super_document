# ExecParallelRetrieveJitInstrumentation

## Location
src/backend/executor/execParallel.c: 1091 - 1130

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
  - MemoryContextAllocZero
  - InstrJitAgg  
  - mul_size
  - MemoryContextAlloc
  - memcpy
- Called from (representative examples):
  - ExecParallelCleanup

## Notes and Other Information
- This is a static function internal to execParallel.c
- Memory allocation occurs in the per-query memory context (es_query_cxt) to ensure proper lifetime management
- The function only executes when JIT compilation is enabled and parallel workers have generated JIT instrumentation data
- Provides essential data for understanding JIT compilation overhead and effectiveness in parallel queries
- Works in conjunction with the regular instrumentation retrieval to provide comprehensive performance analysis
- The combined instrumentation (es_jit_worker_instr) is lazily allocated only when needed