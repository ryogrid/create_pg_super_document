# cost_bitmap_heap_scan

## Location
src/backend/optimizer/path/costsize.c: 1013 - 1113

## Overview
Determines and returns the cost of scanning a relation using a bitmap index-then-heap scan plan, considering both index access costs and heap tuple retrieval costs.

## Definition
```c
void cost_bitmap_heap_scan(Path *path, PlannerInfo *root, RelOptInfo *baserel,
                          ParamPathInfo *param_info,
                          Path *bitmapqual, double loop_count)
```

## Detailed Description
This function calculates the total cost of executing a bitmap heap scan, which consists of two phases: first building a bitmap from index scans, then using that bitmap to fetch heap tuples. The costing model accounts for:

1. **Index Phase**: Incorporates the total cost of building the bitmap from the index qualification tree
2. **Heap Phase**: Models the cost of fetching pages from the heap based on the bitmap, using a sophisticated cost interpolation between random and sequential access patterns
3. **CPU Processing**: Estimates CPU costs for tuple processing and qualification checking
4. **Parallelism**: Adjusts costs when parallel workers are involved

The function uses a nonlinear interpolation formula to determine page access costs, transitioning from random access costs for small page counts to sequential access costs when nearly the entire table is being scanned.

## Parameters / Member Variables
- `path`: The path node to store the calculated costs and row estimates
- `root`: The PlannerInfo containing global planning context
- `baserel`: The base relation being scanned
- `param_info`: ParamPathInfo for parameterized paths, or NULL for non-parameterized paths
- `bitmapqual`: Tree of IndexPaths, BitmapAndPaths, and BitmapOrPaths representing the index qualification
- `loop_count`: Number of repetitions of the scan for caching behavior estimates

## Dependencies
- Functions called/Symbols referenced:
  - [compute_bitmap_pages](compute_bitmap_pages.md)
  - [get_tablespace_page_costs](../g/get_tablespace_page_costs.md)
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md)
  - [get_parallel_divisor](../g/get_parallel_divisor.md)
  - [clamp_row_est](clamp_row_est.md)

- Called from:
  - [bitmap_scan_cost_est](../b/bitmap_scan_cost_est.md) (in indxpath.c:1546)
  - [create_bitmap_heap_path](create_bitmap_heap_path.md) (in pathnode.c:1063)

## Notes and Other Information
- Only applicable to base relations (asserts RTE_RELATION)
- Assumes index qualifications will always be rechecked at tuple level for simplicity
- Uses sophisticated cost interpolation: `cost_per_page = spc_random_page_cost - (spc_random_page_cost - spc_seq_page_cost) * sqrt(pages_fetched / T)`
- Includes disable_cost penalty when enable_bitmapscan is false
- Handles parallel execution by dividing CPU costs among workers
- Located in src/backend/optimizer/path/costsize.c:1013-1113