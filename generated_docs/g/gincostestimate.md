# gincostestimate

## Location
src/backend/utils/adt/selfuncs.c: 7649 - 8038

## Overview
Main cost estimation function for GIN (Generalized Inverted Index) access paths in PostgreSQL's query planner.

## Definition
void gincostestimate(PlannerInfo *root, IndexPath *path, double loop_count, Cost *indexStartupCost, Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation, double *indexPages)

## Detailed Description
The gincostestimate function provides comprehensive cost estimation for GIN index scans, which have fundamentally different search behavior compared to other index types. It retrieves statistical information from the index's meta page, analyzes each index clause to determine search patterns, and calculates costs based on the unique structure of GIN indexes. The function handles entry pages (containing the search keys), data pages (containing tuple pointers), and pending pages (from recent insertions). It estimates costs for tree descent, page fetches, partial matches, and accounts for ScalarArrayOp expressions that generate multiple scans. The cost model considers cache effects, random page access costs, and CPU costs for processing search entries and result tuples.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and statistics
- `path`: IndexPath representing the specific GIN index access path being costed
- `loop_count`: Expected number of times this index scan will be executed in a nestloop
- `indexStartupCost`: Output parameter for one-time startup cost of the index scan
- `indexTotalCost`: Output parameter for total cost including startup and per-tuple costs
- `indexSelectivity`: Output parameter for estimated selectivity of the index condition
- `indexCorrelation`: Output parameter for correlation between index and heap order (always 0.0 for GIN)
- `indexPages`: Output parameter for estimated number of data pages to be accessed

## Dependencies
- Functions called/Symbols referenced:
  - [get_quals_from_indexclauses](get_quals_from_indexclauses.md)
  - [index_open](../i/index_open.md)
  - [ginGetStats](ginGetStats.md)
  - [index_close](../i/index_close.md)
  - [add_predicate_to_index_quals](../a/add_predicate_to_index_quals.md)
  - [clauselist_selectivity](../c/clauselist_selectivity.md)
  - [get_tablespace_page_costs](get_tablespace_page_costs.md)
  - [gincost_opexpr](gincost_opexpr.md)
  - [gincost_scalararrayopexpr](gincost_scalararrayopexpr.md)
  - [index_pages_fetched](../i/index_pages_fetched.md)
  - [index_other_operands_eval_cost](../i/index_other_operands_eval_cost.md)
  - GinStatsData
  - GinQualCounts
- Called from (representative examples):
  - [ginhandler](ginhandler.md)

## Notes and Other Information
- Retrieves actual statistics from GIN index meta page when available, falls back to heuristic estimates for hypothetical indexes
- Scales statistics based on index growth since last VACUUM, with fallback heuristics for excessive growth
- Assumes 90% entry pages, 10% data pages, and 100 entries per entry page when statistics are unavailable
- Handles full index scan cases where certain search modes require scanning all entries
- Models entry tree descent costs using logarithmic complexity similar to B-tree indexes
- Uses power function (numEntryPages^0.15) to estimate entry pages fetched during searches
- Accounts for partial match algorithm costs which require scanning leaf entry pages
- Applies cache effects modeling for multiple scans due to nestloops or array operations
- Uses random page cost since logically close pages may be physically distant on disk
- Includes cross-check based on overall selectivity to avoid under-estimation with high key frequency
- Always sets indexCorrelation to 0.0 since GIN indexes don't maintain tuple order correlation