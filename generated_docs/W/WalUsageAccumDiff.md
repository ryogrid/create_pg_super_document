# WalUsageAccumDiff

## Location
src/backend/executor/instrument.c: 286 - 291

## Overview
A utility function that accumulates Write-Ahead Log (WAL) usage statistics by computing the difference between two WalUsage snapshots and adding that difference to a destination WalUsage structure.

## Definition


## Detailed Description
WalUsageAccumDiff is a public function that calculates the incremental WAL usage statistics between two points in time and accumulates those differences into a destination structure. It performs the operation dst += (add - sub) for all WAL usage counters.

This function is essential for PostgreSQL's query instrumentation system, allowing the database to track how much WAL activity occurred during specific operations or time periods. Since WalUsage counters are monotonically increasing and never reset, this difference-based approach enables precise measurement of WAL resource consumption for individual query operations.

The function handles all three WAL metrics: the number of WAL records produced, the number of full page images, and the total size in bytes of WAL records generated during the measured period.

## Parameters / Member Variables
- : Pointer to the destination WalUsage structure that will accumulate the computed differences
- : Pointer to the WalUsage structure representing the ending state (higher counter values)
- : Pointer to the WalUsage structure representing the starting state (lower counter values)

## Dependencies
- Functions called/Symbols referenced:
  - WalUsage (struct type definition)
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - InstrStopNode
  - [InstrEndParallelQuery](../I/InstrEndParallelQuery.md)
  - pgstat_flush_wal

## Notes and Other Information
- This is a public function (non-static), making it accessible from other compilation units
- The function is crucial for PostgreSQL's instrumentation infrastructure to show WAL usage statistics
- Used extensively in query execution instrumentation and vacuum operations
- The difference calculation assumes that 'add' contains values greater than or equal to 'sub'
- Essential for parallel query execution where WAL statistics from multiple workers need to be aggregated
- Tracks only WAL activity that can be meaningfully measured per query, not global WAL activities
- Located in src/backend/executor/instrument.c:286-291