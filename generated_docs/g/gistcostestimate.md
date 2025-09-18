# gistcostestimate

## Location
src/backend/utils/adt/selfuncs.c: 7239 - 7293

## Overview
A cost estimation function for GiST (Generalized Search Tree) index access paths that extends generic cost estimation with GiST-specific tree descent modeling and fanout assumptions.

## Definition


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
- : PlannerInfo structure containing global planning context and statistics
- : IndexPath structure describing the specific GiST index access path being costed
- : Expected number of times this index scan will be executed (for nested loops)
- : Output parameter for one-time startup cost of the index scan
- : Output parameter for total cost including per-tuple processing
- : Output parameter for estimated fraction of table rows that will be returned
- : Output parameter for correlation between index and table ordering (inherited from generic estimation)
- : Output parameter for estimated number of index pages to be accessed

## Dependencies
- Functions called/Symbols referenced:
  - genericcostestimate
  - log (math function)
  - ceil (math function)
  - DEFAULT_PAGE_CPU_MULTIPLIER
- Called from (representative examples):
  - gisthandler (GiST access method handler)

## Notes and Other Information
- Assumes a fanout of 100 for tree height calculations, which is cached in index->tree_height
- Uses natural logarithm (log) instead of binary logarithm (log2) to account for variable branching factors
- Charges descent costs once per ScalarArrayOpExpr scan, similar to B-tree handling
- Includes both comparison costs (log(N) operations) and page processing costs (tree_height + 1 pages)
- Tree height calculation avoids computing log(0) by checking for single-page indexes
- Per-page CPU costs are calculated using the same multiplier as B-trees (DEFAULT_PAGE_CPU_MULTIPLIER)
- The cost model is conservative and may overestimate costs for GiST indexes with higher actual fanouts