# HashInstrumentation

## Location
src/include/nodes/execnodes.h: 2721 - 2728

## Overview
HashInstrumentation is a structure that collects and stores performance metrics for hash operations, specifically used for display by PostgreSQL's EXPLAIN ANALYZE command.

## Definition


## Detailed Description
HashInstrumentation serves as a data collection structure for monitoring and analyzing the performance characteristics of hash operations in PostgreSQL. It captures both planned estimates and actual execution metrics, enabling users to understand how hash operations performed compared to initial estimates. This information is particularly valuable for query optimization and performance tuning, as it reveals whether hash operations required more buckets, batches, or memory than originally planned.

## Parameters / Member Variables
- `nbuckets`: The actual number of hash buckets used at the end of execution
- `nbuckets_original`: The originally planned number of hash buckets before execution began
- `nbatch`: The actual number of batches required at the end of execution
- `nbatch_original`: The originally planned number of batches before execution began
- `space_peak`: The peak memory usage in bytes during hash operation execution

## Dependencies
- Functions called/Symbols referenced:
  - Size (for memory measurement)
- Called from (representative examples):
  - [show_hash_info](../s/show_hash_info.md) (for EXPLAIN ANALYZE output)
  - ExecHashRetrieveInstrumentation (for collecting metrics)
  - ExecHashAccumInstrumentation (for accumulating metrics)
  - ExecHashEstimate (for estimation)
  - ExecShutdownHash (during cleanup)

## Notes and Other Information
- Essential component of PostgreSQL's query performance analysis infrastructure
- Enables comparison between planned and actual hash operation characteristics
- Used primarily in conjunction with EXPLAIN ANALYZE to provide detailed hash operation insights
- Helps identify cases where hash operations exceed planned resource usage
- The distinction between original and final values helps detect when hash operations required dynamic resizing or rebatching
- Part of the instrumentation framework that supports PostgreSQL's cost-based query optimizer
- Referenced by SharedHashInfo and HashState structures for parallel and general hash operations
- Located in src/include/nodes/execnodes.h:2721-2728