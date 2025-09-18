# InstrStartParallelQuery

## Location
src/backend/executor/instrument.c: 200 - 207

## Overview
Captures baseline buffer usage and WAL usage statistics at the start of parallel query execution for later comparison and instrumentation.

## Definition
void InstrStartParallelQuery(void)

## Detailed Description
InstrStartParallelQuery is a simple but essential function that establishes baseline measurements for buffer and WAL usage tracking during parallel query execution. It saves the current global statistics into static variables that can later be used to calculate the delta of resource usage attributable to the parallel query execution. This function is typically called at the beginning of parallel worker processes to establish their starting resource usage baseline.

## Parameters / Member Variables
This function takes no parameters and operates on global state variables.

## Dependencies
- Functions called/Symbols referenced:
  - pgBufferUsage (global variable for current buffer usage statistics)
  - pgWalUsage (global variable for current WAL usage statistics)
  - save_pgBufferUsage (static variable to store baseline buffer usage)
  - save_pgWalUsage (static variable to store baseline WAL usage)
- Called from (representative examples):
  - [_brin_parallel_build_main](../b/_brin_parallel_build_main.md) (in brin.c for parallel BRIN index building)
  - [_bt_parallel_build_main](../b/_bt_parallel_build_main.md) (in nbtsort.c for parallel B-tree index building)
  - [parallel_vacuum_main](../p/parallel_vacuum_main.md) (in vacuumparallel.c for parallel vacuum operations)
  - [ParallelQueryMain](../P/ParallelQueryMain.md) (in execParallel.c for general parallel query execution)

## Notes and Other Information
- This function is called once per parallel worker process during initialization
- Works in conjunction with InstrEndParallelQuery and InstrAccumParallelQuery to track resource usage
- The saved baseline values are used to calculate incremental resource consumption during parallel operations
- Essential for accurate instrumentation in parallel execution contexts where multiple processes contribute to overall resource usage
- Part of PostgreSQLs parallel execution infrastructure that enables efficient multi-process query processing