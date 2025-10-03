# ExecProcNodeInstr

## Location
[src/backend/executor/execProcnode.c:474-501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execProcnode.c#L474-L501)

## Overview
ExecProcNodeInstr is a wrapper function that adds performance instrumentation around plan node execution, measuring timing and tuple count statistics.

## Definition

```c
static TupleTableSlot *
ExecProcNodeInstr(PlanState *node)
```
## Detailed Description
ExecProcNodeInstr provides a lightweight instrumentation wrapper for plan node execution when performance monitoring is enabled. The function implements a clean separation between normal execution and instrumented execution, ensuring that the overhead of instrumentation only affects queries when monitoring is explicitly requested.

The instrumentation process follows a simple pattern:
1. Calls InstrStartNode to begin timing measurement for the execution
2. Delegates to the real execution function (node->ExecProcNodeReal)
3. Calls InstrStopNode to end timing measurement and record tuple count

The tuple count measurement is based on whether the result slot contains a valid tuple (1.0 for non-null results, 0.0 for null results). This provides essential metrics for query performance analysis, including execution time per node and tuple throughput.

By keeping instrumentation in a separate wrapper function, PostgreSQL avoids any performance overhead in the normal case where no instrumentation is wanted, while providing detailed performance metrics when needed for query optimization and debugging.

## Parameters / Member Variables
- `*node`: The PlanState node being executed with instrumentation enabled
## Dependencies
- Functions called/Symbols referenced:
  - [InstrStartNode](../I/InstrStartNode.md) (begin instrumentation timing)
  - [InstrStopNode](../I/InstrStopNode.md) (end instrumentation timing and record tuple count)
  - TupIsNull (check if result tuple is null)
- Called from (representative examples):
  - [ExecProcNodeFirst](ExecProcNodeFirst.md) (when instrumentation is enabled)

## Notes and Other Information
- This is a static function, only visible within execProcnode.c
- Returns a TupleTableSlot pointer like all ExecProcNode functions
- The function adds minimal overhead while providing essential performance metrics
- Tuple counting uses 1.0 for valid tuples and 0.0 for null results
- Only used when estate->es_instrument is enabled during node initialization
- Separating instrumentation into its own wrapper avoids overhead when monitoring is disabled

## Simplified Source
```c
static TupleTableSlot *ExecProcNodeInstr(PlanState *node) {
    // Start performance instrumentation timing
    InstrStartNode(node->instrument);

    // Execute the actual plan node
    TupleTableSlot *result = node->ExecProcNodeReal(node);

    // Stop instrumentation and record tuple count (1 for valid tuple, 0 for null)
    InstrStopNode(node->instrument, TupIsNull(result) ? 0.0 : 1.0);

    return result;
}
```