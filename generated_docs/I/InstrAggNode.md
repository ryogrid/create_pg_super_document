# InstrAggNode

## Location
src/backend/executor/instrument.c: 169 - 199

## Overview
Aggregates instrumentation information from one Instrumentation structure into another, combining execution statistics for performance monitoring and analysis.

## Definition
void InstrAggNode(Instrumentation *dst, Instrumentation *add)

## Detailed Description
InstrAggNode is a utility function that merges instrumentation data from a source Instrumentation structure into a destination structure. This function is crucial for parallel query execution where multiple workers collect separate instrumentation data that needs to be aggregated into a consolidated view. The function handles various timing metrics, tuple counts, buffer usage, and WAL usage statistics while preserving the earliest firsttuple timing when both structures are running.

## Parameters / Member Variables
- dst: Pointer to the destination Instrumentation structure where aggregated data will be stored
- add: Pointer to the source Instrumentation structure containing data to be added to the destination

## Dependencies
- Functions called/Symbols referenced:
  - INSTR_TIME_ADD (macro for adding time values)
  - [BufferUsageAdd](../B/BufferUsageAdd.md) (function for aggregating buffer usage statistics)
  - [WalUsageAdd](../W/WalUsageAdd.md) (function for aggregating WAL usage statistics)
- Called from (representative examples):
  - [ExecParallelRetrieveInstrumentation](../E/ExecParallelRetrieveInstrumentation.md) (in execParallel.c for retrieving parallel execution stats)
  - [ExecParallelReportInstrumentation](../E/ExecParallelReportInstrumentation.md) (in execParallel.c for reporting parallel execution stats)

## Notes and Other Information
- Handles running state propagation: if destination is not running but source is, destination becomes running
- Preserves the earliest firsttuple time when both structures are running
- Aggregates all numeric counters: tuplecount, startup, total, ntuples, ntuples2, nloops, nfiltered1, nfiltered2
- Conditionally aggregates buffer usage statistics only if dst->need_bufusage is true
- Conditionally aggregates WAL usage statistics only if dst->need_walusage is true
- Essential for parallel query execution where worker instrumentation data must be consolidated
- Used in PostgreSQLs EXPLAIN ANALYZE functionality to provide unified performance statistics across parallel workers