# spgcostestimate

## Location
src/backend/utils/adt/selfuncs.c: 7294 - 7360

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
  - genericcostestimate
  - IndexOptInfo
  - GenericCosts
  - DEFAULT_PAGE_CPU_MULTIPLIER
- Called from (representative examples):
  - spghandler

## Notes and Other Information
- Uses an arbitrary fanout assumption of 100 for tree height calculations
- Caches tree height in index->tree_height to avoid repeated computations
- Models descent costs using logarithmic complexity similar to B-tree indexes
- Adds both CPU costs for tree navigation and page access costs
- Handles edge cases like single-page indexes and empty indexes gracefully
- The cost model accounts for multiple ScalarArrayOp scans via num_sa_scans