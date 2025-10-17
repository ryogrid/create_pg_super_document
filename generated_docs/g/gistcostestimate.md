# gistcostestimate

## Location
[src/backend/utils/adt/selfuncs.c:7239-7293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L7239-L7293)

## Overview
A cost estimation function for GiST (Generalized Search Tree) index access paths that extends generic cost estimation with GiST-specific tree descent modeling and fanout assumptions.

## Definition

```c
void
gistcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				 Cost *indexStartupCost, Cost *indexTotalCost,
				 Selectivity *indexSelectivity, double *indexCorrelation,
				 double *indexPages)
```
## Detailed Description
The  function provides specialized cost estimation for GiST index scans by building upon the generic cost estimation framework and adding GiST-specific tree traversal costs.

Key features include:
- **Tree Height Calculation**: Estimates the GiST tree height using an assumed fanout of 100 nodes per internal page
- **Descent Cost Modeling**: Adds CPU costs for traversing the tree from root to leaf, similar to B-tree but with different fanout assumptions
- **Caching Optimization**: Uses the index's tree_height field to cache computed height values
- **ScalarArrayOpExpr Support**: Properly accounts for multiple index descents when ScalarArrayOpExpr operations are involved

Unlike B-trees which typically have a fanout of hundreds to thousands, GiST trees are assumed to have a more conservative fanout of 100, leading to potentially deeper trees for the same number of pages. The function models this by computing log(N) rather than log2(N) for descent costs, reflecting the variable branching factor of GiST indexes.

The cost model includes both comparison costs during tree traversal and fixed per-page costs for processing each level of the tree, ensuring that the performance impact of tree depth is properly reflected in query planning decisions.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global planning context and statistics
- `*path`: IndexPath structure describing the specific GiST index access path being costed
- `loop_count`: Expected number of times this index scan will be executed (for nested loops)
- `*indexStartupCost`: Output parameter for one-time startup cost of the index scan
- `*indexTotalCost`: Output parameter for total cost including per-tuple processing
- `*indexSelectivity`: Output parameter for estimated fraction of table rows that will be returned
- `*indexCorrelation`: Output parameter for correlation between index and table ordering (inherited from generic estimation)
- `*indexPages`: Output parameter for estimated number of index pages to be accessed
## Dependencies
- Functions called/Symbols referenced:
  - [genericcostestimate](genericcostestimate.md)
  - log (math function)
  - ceil (math function)
  - DEFAULT_PAGE_CPU_MULTIPLIER
- Called from (representative examples):
  - [gisthandler](gisthandler.md) (GiST access method handler)

## Notes and Other Information
- Assumes a fanout of 100 for tree height calculations, which is cached in index->tree_height
- Uses natural logarithm (log) instead of binary logarithm (log2) to account for variable branching factors
- Charges descent costs once per ScalarArrayOpExpr scan, similar to B-tree handling
- Includes both comparison costs (log(N) operations) and page processing costs (tree_height + 1 pages)
- Tree height calculation avoids computing log(0) by checking for single-page indexes
- Per-page CPU costs are calculated using the same multiplier as B-trees (DEFAULT_PAGE_CPU_MULTIPLIER)
- The cost model is conservative and may overestimate costs for GiST indexes with higher actual fanouts

## Simplified Source

```c
void gistcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
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

**Core Logic**: Extends generic cost estimation with GiST-specific tree descent costs, using logarithmic tree height calculation with fanout of 100 and adding both comparison and page processing costs.