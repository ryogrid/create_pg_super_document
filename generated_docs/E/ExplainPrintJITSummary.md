# ExplainPrintJITSummary

## Location
src/backend/commands/explain.c: 985 - 1010

## Overview
Prints summarized JIT instrumentation statistics from both the leader process and parallel workers in EXPLAIN output.

## Definition
```c
void ExplainPrintJITSummary(ExplainState *es, QueryDesc *queryDesc)
```

## Detailed Description
ExplainPrintJITSummary aggregates and displays JIT compilation statistics for query execution plans in EXPLAIN output. The function collects JIT instrumentation data from both the main execution process and any parallel worker processes, providing a comprehensive view of JIT performance metrics.

The function creates a local copy of instrumentation data to avoid modifying the original state, since it may be called multiple times during plan explanation. It aggregates statistics using InstrJitAgg and delegates the actual output formatting to ExplainPrintJIT.

## Parameters / Member Variables
- `es`: ExplainState structure containing formatting and output configuration for EXPLAIN
- `queryDesc`: QueryDesc structure containing query execution state and JIT instrumentation data

## Dependencies
- Functions called/Symbols referenced:
  - InstrJitAgg (aggregates JIT instrumentation statistics)
  - ExplainPrintJIT (formats and prints JIT statistics)
  - JitInstrumentation (struct for JIT metrics)
  - PGJIT_PERFORM (flag indicating JIT was performed)
- Called from (representative examples):
  - ExplainOnePlan (main EXPLAIN plan processing function)

## Notes and Other Information
- Returns early if JIT was not performed (PGJIT_PERFORM flag not set)
- Uses a local copy of instrumentation data to preserve original state
- Handles both single-process and parallel execution scenarios
- Part of PostgreSQLs EXPLAIN infrastructure for displaying query execution details
- Located in src/backend/commands/explain.c:985-1010