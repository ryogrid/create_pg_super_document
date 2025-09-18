# cost_index

## Location
[src/backend/optimizer/path/costsize.c:549-839](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L549-L839)

## Overview
Calculates the comprehensive cost estimate for scanning a relation using an index, including both index access costs and heap tuple retrieval costs.

## Definition
```c
void cost_index(IndexPath *path, PlannerInfo *root, double loop_count,
                bool partial_path)
```

## Detailed Description
The `cost_index` function determines the total cost of executing an index scan operation, which is one of the most complex costing functions in PostgreSQL's optimizer. It calculates costs for both accessing the index itself and fetching the corresponding heap tuples. The function uses access method-specific cost estimation via the index's amcostestimate function, then applies sophisticated models for heap page access costs based on index correlation with heap order.

The function handles multiple scenarios including regular index scans, index-only scans, parameterized paths, and parallel execution. It uses the Mackert and Lohman formula for uncorrelated access patterns and interpolates between random and sequential costs based on index-heap correlation. For index-only scans, it accounts for visibility map information to reduce estimated heap page fetches.

## Parameters / Member Variables
- `path`: The IndexPath structure to populate with cost estimates and configuration
- `root`: PlannerInfo containing global planning context and configuration  
- `loop_count`: Number of repetitions of the indexscan for caching behavior estimation
- `partial_path`: Boolean indicating whether this is a partial path for parallel execution

## Dependencies
- Functions called/Symbols referenced:
  - [IndexPath](../I/IndexPath.md) (structure)
  - [IndexOptInfo](../I/IndexOptInfo.md) (structure)
  - Cost (type)
  - QualCost (structure)
  - [list_concat](../l/list_concat.md)
  - [extract_nonindex_conditions](../e/extract_nonindex_conditions.md)
  - [clamp_row_est](clamp_row_est.md)
  - [get_tablespace_page_costs](../g/get_tablespace_page_costs.md)
  - [index_pages_fetched](../i/index_pages_fetched.md)
  - [compute_parallel_worker](compute_parallel_worker.md)
  - [cost_qual_eval](cost_qual_eval.md)
  - [get_parallel_divisor](../g/get_parallel_divisor.md)
  - RTE_RELATION (constant)
- Called from (representative examples):
  - [create_index_path](create_index_path.md)
  - [reparameterize_path](../r/reparameterize_path.md)

## Notes and Other Information
This function implements one of the most sophisticated costing models in PostgreSQL, handling the complex relationship between index selectivity, heap page correlation, and access patterns. It distinguishes between startup costs (index initialization and qualification setup) and run costs (per-tuple processing and I/O). The correlation-based interpolation between random and sequential access costs is crucial for accurate cost estimation, especially for clustered tables. For parallel execution, it adjusts both worker count estimation and cost distribution among workers.