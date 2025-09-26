# InstrJitAgg

## Location
[src/backend/jit/jit.c:182-190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/jit.c#L182-L190)

## Overview
Aggregates JIT instrumentation statistics by adding the metrics from one JitInstrumentation structure to another, used for collecting and combining JIT compilation performance data across multiple operations or parallel workers.

## Definition

```c
void
InstrJitAgg(JitInstrumentation *dst, JitInstrumentation *add)
```
## Detailed Description
The  function performs aggregation of JIT (Just-In-Time) compilation instrumentation data by combining statistics from a source  structure into a destination structure. This function is essential for PostgreSQL's JIT performance monitoring system, allowing the database to collect comprehensive statistics about JIT compilation performance across different execution contexts.

The function accumulates both count-based metrics (number of created functions) and time-based metrics (various compilation phase timings) using appropriate addition operations. For timing counters, it uses the  macro to properly handle the platform-specific timing instrumentation format.

This aggregation capability is particularly important in parallel query execution scenarios where multiple workers may be performing JIT compilation independently, and their statistics need to be combined to provide a complete picture of JIT performance for the entire query.

## Parameters / Member Variables
- : Pointer to the destination JitInstrumentation structure where aggregated results will be stored
- : Pointer to the source JitInstrumentation structure containing values to be added to the destination

## Dependencies
- Functions called/Symbols referenced:
  - [JitInstrumentation](../J/JitInstrumentation.md) (struct type)
  - INSTR_TIME_ADD (macro for timing accumulation)
- Called from (representative examples):
  - [ExplainPrintJITSummary](../E/ExplainPrintJITSummary.md) (src/backend/commands/explain.c:997, 1001)
  - [ExecParallelRetrieveJitInstrumentation](../E/ExecParallelRetrieveJitInstrumentation.md) (src/backend/executor/execParallel.c:1110)

## Notes and Other Information
- This function modifies the destination structure in-place, adding values from the source structure
- The function handles both simple integer addition for  and specialized timing addition for all timing counters
- Used extensively in EXPLAIN output generation to show consolidated JIT statistics
- Critical for parallel query execution where JIT statistics from multiple workers need to be combined
- The timing counters being aggregated include:
  - : Total time spent generating JIT code
  - : Time spent on tuple deformation (subset of generation time)
  - : Time spent on function inlining optimization
  - : Time spent on code optimization
  - : Time spent emitting final machine code
- Located in src/backend/jit/jit.c:182-190