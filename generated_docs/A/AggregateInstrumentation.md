# AggregateInstrumentation

## Location
src/include/nodes/execnodes.h: 2427 - 2432

## Overview
AggregateInstrumentation is a structure that captures performance metrics for hash aggregate operations, tracking memory and disk usage statistics per worker.

## Definition


## Detailed Description
AggregateInstrumentation is a specialized structure designed to collect and store performance instrumentation data for hash aggregate operations in PostgreSQL. This structure captures key metrics that help analyze the performance characteristics of hash-based aggregation, particularly focusing on memory consumption and disk spill behavior.

Hash aggregation is a common strategy for implementing GROUP BY operations where the executor builds an in-memory hash table to accumulate aggregate values. When the hash table becomes too large to fit in memory (work_mem), PostgreSQL may spill data to disk using a batching strategy. The AggregateInstrumentation structure tracks these important performance aspects, providing valuable insights for query optimization and performance analysis.

This structure is particularly useful in parallel execution contexts where multiple workers may be performing hash aggregation simultaneously, and their individual performance metrics need to be collected and potentially aggregated for comprehensive analysis.

## Parameters / Member Variables
- : The peak memory usage of the hash table during execution, measured in bytes (Size type)
- : The amount of disk space used for spilling hash table data when memory is exhausted, measured in kilobytes
- : The total number of batches used throughout the entire execution when the hash table needed to be partitioned due to memory constraints

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references - uses basic data types)
- Called from (representative examples):
  - [show_hashagg_info](../s/show_hashagg_info.md) (src/backend/commands/explain.c:3545)
  - [ExecEndAgg](../E/ExecEndAgg.md) (src/backend/executor/nodeAgg.c:4318)
  - [ExecAggEstimate](../E/ExecAggEstimate.md) (src/backend/executor/nodeAgg.c:4691)
  - [ExecAggInitializeDSM](../E/ExecAggInitializeDSM.md) (src/backend/executor/nodeAgg.c:4713)
  - [ExecAggRetrieveInstrumentation](../E/ExecAggRetrieveInstrumentation.md) (src/backend/executor/nodeAgg.c:4751)
  - [SharedAggInfo](../S/SharedAggInfo.md) (src/include/nodes/execnodes.h:2441)

## Notes and Other Information
- This structure is primarily used for EXPLAIN ANALYZE output to provide detailed statistics about hash aggregate performance
- The memory peak measurement helps identify queries that may benefit from increased work_mem settings
- Disk usage tracking is crucial for understanding when and how much data spills to temporary files
- Batch count information indicates the level of partitioning required due to memory pressure
- Used in both serial and parallel execution contexts, with SharedAggInfo coordinating data from multiple workers
- Essential for performance tuning of aggregation-heavy queries and understanding resource utilization patterns
- The metrics collected help DBAs and developers optimize queries and system configuration for better aggregation performance