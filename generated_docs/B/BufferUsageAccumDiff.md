# BufferUsageAccumDiff

## Location
[src/backend/executor/instrument.c:248-277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/instrument.c#L248-L277)

## Overview
A utility function that accumulates buffer usage statistics by computing the difference between two BufferUsage snapshots and adding that difference to a destination BufferUsage structure.

## Definition

```c
void
BufferUsageAccumDiff(BufferUsage *dst,
					 const BufferUsage *add,
					 const BufferUsage *sub)
```
## Detailed Description
BufferUsageAccumDiff is a public function that calculates the incremental buffer usage statistics between two points in time and accumulates those differences into a destination structure. It performs the operation dst += (add - sub) for all buffer usage counters and timing measurements.

This function is essential for PostgreSQL's query instrumentation system, allowing the database to track how much buffer activity occurred during specific operations or time periods. Since BufferUsage counters are monotonically increasing and never reset, this difference-based approach enables precise measurement of resource consumption for individual query operations.

The function handles all three categories of buffer usage (shared, local, and temporary) and their associated timing measurements, using specialized macros for proper time arithmetic.

## Parameters / Member Variables
- : Pointer to the destination BufferUsage structure that will accumulate the computed differences
- : Pointer to the BufferUsage structure representing the ending state (higher counter values)
- : Pointer to the BufferUsage structure representing the starting state (lower counter values)

## Dependencies
- Functions called/Symbols referenced:
  - INSTR_TIME_ACCUM_DIFF (macro for computing and accumulating timing differences)
  - BufferUsage (struct type definition)
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - [standard_ExplainOneQuery](../s/standard_ExplainOneQuery.md)
  - [serializeAnalyzeReceive](../s/serializeAnalyzeReceive.md)
  - [ExplainExecuteQuery](../E/ExplainExecuteQuery.md)
  - InstrStopNode
  - [InstrEndParallelQuery](../I/InstrEndParallelQuery.md)

## Notes and Other Information
- This is a public function (non-static), making it accessible from other compilation units
- The function is crucial for PostgreSQL's EXPLAIN functionality to show buffer usage statistics
- Used extensively in query execution instrumentation to measure resource consumption
- The difference calculation assumes that 'add' contains values greater than or equal to 'sub'
- Essential for parallel query execution where statistics from multiple workers need to be aggregated
- Located in src/backend/executor/instrument.c:248-277