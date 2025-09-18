# final_cost_nestloop

## Location
[src/backend/optimizer/path/costsize.c:3308-3513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L3308-L3513)

## Overview
Provides the final estimate of the cost and result size of a nestloop join path, performing detailed cost calculations including CPU costs and join qualification evaluation.

## Definition


## Detailed Description
This function performs the second phase of nested loop join cost estimation in PostgreSQL's query planner, building upon the preliminary estimates from initial_cost_nestloop. It provides comprehensive cost analysis including:

1. **Row count finalization**: Sets the final row estimate, accounting for parameterized paths and parallel execution scaling.

2. **Disable cost handling**: Adds disable_cost if nested loop joins are disabled via enable_nestloop.

3. **Special join type optimization**: For SEMI/ANTI joins or unique inner relations, calculates optimized costs based on early termination behavior:
   - Estimates scan fraction based on match distribution
   - Differentiates between indexed and non-indexed join scenarios
   - Accounts for unmatched outer rows requiring full inner scans

4. **CPU cost calculation**: Evaluates join restriction qualifications and adds per-tuple CPU costs including tuple processing overhead.

5. **Target list evaluation**: Adds costs for evaluating the output target list per result row.

The function handles complex scenarios like indexed join qualifications where unmatched rows may result in very cheap index probes returning no results.

## Parameters / Member Variables
- : PlannerInfo structure containing planner context and statistics
- : NestPath structure to be finalized with cost and row estimates
- : JoinCostWorkspace containing preliminary estimates from initial_cost_nestloop
- : JoinPathExtraData containing miscellaneous join information including semifactors

## Dependencies
- Functions called/Symbols referenced:
  - [get_parallel_divisor](../g/get_parallel_divisor.md)
  - [clamp_row_est](../c/clamp_row_est.md)
  - [has_indexed_join_quals](../h/has_indexed_join_quals.md)
  - [cost_qual_eval](../c/cost_qual_eval.md)
  - NestPath
  - [JoinCostWorkspace](../J/JoinCostWorkspace.md)
  - [JoinPathExtraData](../J/JoinPathExtraData.md)
  - QualCost
  - Cost
  - JOIN_SEMI
  - JOIN_ANTI
- Called from (representative examples):
  - [create_nestloop_path](../c/create_nestloop_path.md)

## Notes and Other Information
- This is the second phase of the two-phase nested loop costing process
- Handles complex optimization for SEMI/ANTI joins with early scan termination
- Uses a fuzz factor of 2.0 when estimating scan fractions for matched rows
- Distinguishes between indexed and non-indexed join scenarios for cost accuracy
- Protects against zero row count assumptions that could cause division errors
- Accounts for parallel execution by scaling row estimates appropriately
- Target list evaluation costs are applied per output row, not per processed tuple