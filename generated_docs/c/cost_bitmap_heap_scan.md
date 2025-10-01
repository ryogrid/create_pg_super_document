# cost_bitmap_heap_scan

## Location
[src/backend/optimizer/path/costsize.c:1013-1113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1013-L1113)

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

## Simplified Source

```c
void
cost_bitmap_heap_scan(Path *path, PlannerInfo *root, RelOptInfo *baserel,
                     ParamPathInfo *param_info, Path *bitmapqual, double loop_count)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    Cost indexTotalCost;
    QualCost qpqual_cost;
    Cost cpu_per_tuple;
    Cost cost_per_page;
    double tuples_fetched;
    double pages_fetched;
    double spc_seq_page_cost, spc_random_page_cost;
    double T;

    // Validate this is a base relation
    Assert(IsA(baserel, RelOptInfo));
    Assert(baserel->relid > 0);
    Assert(baserel->rtekind == RTE_RELATION);

    // Set row estimate
    if (param_info)
        path->rows = param_info->ppi_rows;
    else
        path->rows = baserel->rows;

    // Add disable cost if bitmap scans are disabled
    if (!enable_bitmapscan)
        startup_cost += disable_cost;

    // Compute bitmap pages and index costs
    pages_fetched = compute_bitmap_pages(root, baserel, bitmapqual,
                                        loop_count, &indexTotalCost, &tuples_fetched);

    startup_cost += indexTotalCost;
    T = (baserel->pages > 1) ? (double) baserel->pages : 1.0;

    // Get tablespace page costs
    get_tablespace_page_costs(baserel->reltablespace,
                             &spc_random_page_cost, &spc_seq_page_cost);

    // Calculate cost per page using nonlinear interpolation
    if (pages_fetched >= 2.0)
        cost_per_page = spc_random_page_cost -
            (spc_random_page_cost - spc_seq_page_cost) * sqrt(pages_fetched / T);
    else
        cost_per_page = spc_random_page_cost;

    run_cost += pages_fetched * cost_per_page;

    // Calculate CPU costs for tuple processing
    get_restriction_qual_cost(root, baserel, param_info, &qpqual_cost);
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;

    // Adjust for parallelism if workers are used
    if (path->parallel_workers > 0) {
        double parallel_divisor = get_parallel_divisor(path);
        cpu_per_tuple /= parallel_divisor;
        path->rows = clamp_row_est(path->rows / parallel_divisor);
    }

    run_cost += cpu_per_tuple * tuples_fetched;

    // Add target list evaluation costs
    startup_cost += path->pathtarget->cost.startup;
    run_cost += path->pathtarget->cost.per_tuple * path->rows;

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
}
```