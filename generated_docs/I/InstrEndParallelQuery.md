# InstrEndParallelQuery

## Location
src/backend/executor/instrument.c: 208 - 217

## Overview
Calculates and reports the incremental buffer usage and WAL usage consumed during parallel query execution by computing the difference from baseline measurements.

## Definition
void InstrEndParallelQuery(BufferUsage *bufusage, WalUsage *walusage)

## Detailed Description
InstrEndParallelQuery finalizes parallel query instrumentation by calculating the actual resource consumption that occurred during the parallel execution phase. It computes the difference between current resource usage and the baseline established by InstrStartParallelQuery, providing accurate measurements of resources consumed specifically by the parallel operation. The function populates the provided BufferUsage and WalUsage structures with the calculated deltas.

## Parameters / Member Variables
- bufusage: Pointer to BufferUsage structure that will be populated with incremental buffer usage statistics
- walusage: Pointer to WalUsage structure that will be populated with incremental WAL usage statistics

## Dependencies
- Functions called/Symbols referenced:
  - memset (standard C function for memory initialization)
  - [BufferUsageAccumDiff](../B/BufferUsageAccumDiff.md) (function to calculate buffer usage difference)
  - [WalUsageAccumDiff](../W/WalUsageAccumDiff.md) (function to calculate WAL usage difference)
  - pgBufferUsage (global variable for current buffer usage)
  - pgWalUsage (global variable for current WAL usage)
  - save_pgBufferUsage (static baseline buffer usage from InstrStartParallelQuery)
  - save_pgWalUsage (static baseline WAL usage from InstrStartParallelQuery)
- Called from (representative examples):
  - [_brin_parallel_build_main](../b/_brin_parallel_build_main.md) (in brin.c for parallel BRIN index building)
  - [_bt_parallel_build_main](../b/_bt_parallel_build_main.md) (in nbtsort.c for parallel B-tree index building)
  - [parallel_vacuum_main](../p/parallel_vacuum_main.md) (in vacuumparallel.c for parallel vacuum operations)
  - [ParallelQueryMain](../P/ParallelQueryMain.md) (in execParallel.c for general parallel query execution)

## Notes and Other Information
- Must be called after InstrStartParallelQuery to provide meaningful delta calculations
- Initializes output structures to zero before calculating differences to ensure clean results
- Works as part of a trio with InstrStartParallelQuery and InstrAccumParallelQuery for complete parallel instrumentation
- Essential for accurate resource usage tracking in parallel execution where multiple workers contribute to overall consumption
- Provides precise measurements by isolating only the resource usage attributable to the parallel operation
- Used in PostgreSQLs instrumentation infrastructure to support EXPLAIN ANALYZE and performance monitoring