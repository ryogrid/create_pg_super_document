# cost_tidscan

## Location
[src/backend/optimizer/path/costsize.c:1249-1356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1249-L1356)

## Overview
Determines and returns the cost of scanning a relation using TIDs (tuple identifiers), calculating both startup and per-tuple costs for TID-based access paths.

## Definition


## Detailed Description
The  function calculates the cost of performing a TID scan on a relation, which is a direct access method that uses tuple identifiers to locate specific rows. This function handles several scenarios including regular TID equality comparisons, TID array operations (ScalarArrayOpExpr), and CURRENT OF expressions used in cursors. The costing model accounts for the fact that each TID typically corresponds to a different page, so random page access costs are applied. Special handling is provided for CURRENT OF expressions, which are forced to use TID scans and have their disable costs subtracted to prevent other scan types from being chosen.

## Parameters / Member Variables
- : Output parameter where the calculated costs will be stored
- : PlannerInfo structure containing global planner state
- : RelOptInfo for the relation being scanned
- : List of TID-checkable qualification clauses
- : ParamPathInfo for parameterized paths, or NULL for non-parameterized paths

## Dependencies
- Functions called/Symbols referenced:
  - [estimate_array_length](../e/estimate_array_length.md)
  - [cost_qual_eval](cost_qual_eval.md)
  - [get_tablespace_page_costs](../g/get_tablespace_page_costs.md)
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md)
  - lsecond
- Called from (representative examples):
  - [create_tidscan_path](create_tidscan_path.md)

## Notes and Other Information
- Only applies to base relations (not joins or subqueries)
- Each TID is assumed to be on a different page, leading to random I/O costs
- CURRENT OF expressions receive special treatment to force TID scan usage
- The enable_tidscan GUC parameter is honored except when CURRENT OF is present
- TID quals are assumed to be a subset of the overall restriction quals
- Array-based TID operations are supported through ScalarArrayOpExpr handling