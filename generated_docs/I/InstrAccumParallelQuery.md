# InstrAccumParallelQuery

## Location
[src/backend/executor/instrument.c:218-225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/instrument.c#L218-L225)

## Overview
Accumulates resource usage statistics from parallel workers into the leaders global statistics, consolidating distributed work measurements.

## Definition
void InstrAccumParallelQuery(BufferUsage *bufusage, WalUsage *walusage)

## Detailed Description
InstrAccumParallelQuery is the final step in parallel query instrumentation that integrates resource usage measurements from parallel worker processes into the leader processs global statistics. This function takes the incremental usage statistics calculated by parallel workers and adds them to the global counters, ensuring that the total resource consumption reflects the combined work of all parallel participants. It is typically called by the leader process after collecting instrumentation data from completed parallel workers.

## Parameters / Member Variables
- bufusage: Pointer to BufferUsage structure containing incremental buffer usage statistics from parallel workers
- walusage: Pointer to WalUsage structure containing incremental WAL usage statistics from parallel workers

## Dependencies
- Functions called/Symbols referenced:
  - [BufferUsageAdd](../B/BufferUsageAdd.md) (function to add buffer usage statistics)
  - [WalUsageAdd](../W/WalUsageAdd.md) (function to add WAL usage statistics)
  - pgBufferUsage (global variable for accumulated buffer usage)
  - pgWalUsage (global variable for accumulated WAL usage)
- Called from (representative examples):
  - [_brin_end_parallel](../b/_brin_end_parallel.md) (in brin.c for finalizing parallel BRIN index building)
  - [_bt_end_parallel](../b/_bt_end_parallel.md) (in nbtsort.c for finalizing parallel B-tree index building)
  - [parallel_vacuum_process_all_indexes](../p/parallel_vacuum_process_all_indexes.md) (in vacuumparallel.c for parallel vacuum completion)
  - [ExecParallelFinish](../E/ExecParallelFinish.md) (in execParallel.c for general parallel query completion)

## Notes and Other Information
- Called by the leader process after parallel workers have completed and reported their resource usage
- Works in conjunction with InstrStartParallelQuery and InstrEndParallelQuery to provide complete parallel instrumentation
- Ensures that global resource usage statistics accurately reflect the total work done by all parallel participants
- Essential for maintaining consistent and accurate system-wide resource usage tracking
- Part of PostgreSQLs parallel execution cleanup and consolidation process
- Critical for proper accounting in EXPLAIN ANALYZE output and system monitoring when parallel execution is involved