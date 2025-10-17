# spgcostestimate

## Location
[src/backend/utils/adt/selfuncs.c:7294-7360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L7294-L7360)

## Overview
Estimates the cost of scanning a SP-GiST (Space-Partitioned Generalized Search Tree) index for query planning in PostgreSQL's optimizer.

## Definition
void spgcostestimate(PlannerInfo *root, IndexPath *path, double loop_count, Cost *indexStartupCost, Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation, double *indexPages)

## Detailed Description
The spgcostestimate function provides cost estimates for SP-GiST index scans to PostgreSQL's query planner. It calculates startup costs, total costs, selectivity, correlation, and page estimates based on the index structure and query parameters. The function models index descent costs similarly to B-tree indexes but uses a fanout assumption of 100 for tree height calculations. It leverages the generic cost estimation framework (genericcostestimate) and adds SP-GiST-specific costs including descent costs based on tree navigation and page access patterns.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and statistics
- : IndexPath representing the specific index access path being costed
- : Expected number of times this index scan will be executed
- : Output parameter for one-time startup cost of the index scan
- : Output parameter for total cost including startup and per-tuple costs
- : Output parameter for estimated selectivity of the index condition
- : Output parameter for correlation between index order and heap order
- : Output parameter for estimated number of index pages to be accessed

## Dependencies
- Functions called/Symbols referenced:
  - [genericcostestimate](../g/genericcostestimate.md)
  - [IndexOptInfo](../I/IndexOptInfo.md)
  - [GenericCosts](../G/GenericCosts.md)
  - DEFAULT_PAGE_CPU_MULTIPLIER
- Called from (representative examples):
  - [spghandler](spghandler.md)

## Notes and Other Information
- Uses an arbitrary fanout assumption of 100 for tree height calculations
- Caches tree height in index->tree_height to avoid repeated computations
- Models descent costs using logarithmic complexity similar to B-tree indexes
- Adds both CPU costs for tree navigation and page access costs
- Handles edge cases like single-page indexes and empty indexes gracefully
- The cost model accounts for multiple ScalarArrayOp scans via num_sa_scans

## Simplified Source

```c
void spgcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                    Cost *indexStartupCost, Cost *indexTotalCost,
                    Selectivity *indexSelectivity, double *indexCorrelation,
                    double *indexPages) {
    IndexOptInfo *index = path->indexinfo;
    GenericCosts costs = {0};
    Cost descentCost;

    // Get base cost estimates using generic estimation
    genericcostestimate(root, path, loop_count, &costs);

    // Calculate tree height using fanout assumption of 100
    if (index->tree_height < 0) {
        if (index->pages > 1)
            index->tree_height = (int) (log(index->pages) / log(100.0));
        else
            index->tree_height = 0;
    }

    // Add CPU cost for tree descent based on number of tuples
    if (index->tuples > 1) {
        descentCost = ceil(log(index->tuples)) * cpu_operator_cost;
        costs.indexStartupCost += descentCost;
        costs.indexTotalCost += costs.num_sa_scans * descentCost;
    }

    // Add per-page cost for tree traversal
    descentCost = (index->tree_height + 1) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.indexStartupCost += descentCost;
    costs.indexTotalCost += costs.num_sa_scans * descentCost;

    // Return computed costs
    *indexStartupCost = costs.indexStartupCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
    *indexPages = costs.numIndexPages;
}
```

**Core Logic**: Estimates SP-GiST index scan costs by extending generic cost estimation with space-partitioned tree-specific descent costs, using the same logarithmic model as GiST with fanout of 100.