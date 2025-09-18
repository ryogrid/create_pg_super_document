# cost_subqueryscan

## Location
[src/backend/optimizer/path/costsize.c:1451-1530](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1451-L1530)

## Overview
Determines and returns the cost of scanning a subquery RTE, calculating costs for accessing results from a subquery as if it were a base relation.

## Definition


## Detailed Description
The `cost_subqueryscan` function calculates the cost of scanning a subquery by building upon the cost of the underlying subplan and adding the overhead of any additional restriction clauses and target list evaluation. The function performs an important optimization: when there are no relevant restriction clauses and the pathtarget is trivial, it recognizes that the SubqueryScan node will likely be optimized away during plan creation, so it returns early without adding overhead costs. For non-trivial cases, it computes row estimates by applying selectivity of restriction clauses to the subpath's row estimate, then adds CPU costs for tuple processing and target list evaluation on top of the subplan's costs.

## Parameters / Member Variables
- `path`: SubqueryScanPath where the calculated costs will be stored
- `root`: PlannerInfo structure containing global planner state
- `baserel`: RelOptInfo for the subquery relation
- `param_info`: ParamPathInfo for parameterized paths, or NULL for non-parameterized paths
- `trivial_pathtarget`: Boolean indicating whether the pathtarget is expected to be trivial

## Dependencies
- Functions called/Symbols referenced:
  - [clamp_row_est](clamp_row_est.md)
  - [clauselist_selectivity](clauselist_selectivity.md)
  - [list_concat_copy](../l/list_concat_copy.md)
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md)
  - JOIN_INNER (constant)
- Called from (representative examples):
  - [create_subqueryscan_path](create_subqueryscan_path.md)

## Notes and Other Information
- Only applies to base relations that are subqueries (RTE_SUBQUERY)
- Row count estimation combines subpath rows with selectivity of restriction clauses
- Includes optimization for trivial cases where the SubqueryScan node may be eliminated
- Handles both parameterized and non-parameterized paths appropriately
- Target list evaluation costs are applied per output row, not per scanned tuple
- The function accounts for potential discrepancies between cost estimates and actual plan structure in edge cases