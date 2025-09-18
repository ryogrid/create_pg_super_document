# compute_parallel_worker

## Location
src/backend/optimizer/path/allpaths.c: 4203 - 4290

## Overview
Calculates the optimal number of parallel workers for scanning a relation based on heap and index size, applying logarithmic scaling and configuration constraints.

## Definition


## Detailed Description
This function determines the appropriate level of parallelism for scanning operations by analyzing the expected workload size. It uses a logarithmic scaling algorithm where the number of workers increases as the scan size grows, with each tripling of pages potentially adding one more worker.

The function respects several configuration parameters and constraints:
- User-specified parallel_workers reloption takes precedence over calculated values
- Minimum thresholds (min_parallel_table_scan_size, min_parallel_index_scan_size) must be met
- For inheritance children, parallel paths are generated even if individual relations are small
- The algorithm prevents integer overflow when calculating thresholds

The computation considers both heap and index scan costs separately, then takes the minimum when both are applicable, ensuring that the limiting factor determines parallelism.

## Parameters / Member Variables
- : RelOptInfo for the relation being analyzed, contains parallel worker preferences and relation type information
- : Expected number of heap pages to scan, or -1 if no heap scan expected
- : Expected number of index pages to scan, or -1 if no index scan expected  
- : Caller-imposed limit on worker count, typically from GUC parameters like max_parallel_workers_per_gather

## Dependencies
- Functions called/Symbols referenced:
  - RELOPT_BASEREL (constant identifying base relations vs inheritance children)
- Called from (representative examples):
  - [create_plain_partial_paths](create_plain_partial_paths.md) (for parallel sequential scans)
  - [create_partial_bitmap_paths](create_partial_bitmap_paths.md) (for parallel bitmap heap scans)
  - [cost_index](cost_index.md) (for index scan costing)
  - [plan_create_index_workers](../p/plan_create_index_workers.md) (for CREATE INDEX operations)

## Notes and Other Information
- Uses logarithmic base-3 scaling: workers increase by 1 for every 3x increase in pages
- Inheritance children can get parallel paths even below minimum thresholds to benefit from sibling aggregation
- The function caps worker count at the caller's maximum to respect system resource limits
- Overflow protection prevents threshold calculations from exceeding INT_MAX/3
- When both heap and index pages are specified, the minimum of the two computed worker counts is used
- The parallel_workers reloption allows per-table override of the calculated parallelism