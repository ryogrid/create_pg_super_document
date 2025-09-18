# cached_plan_cost

## Location
[src/backend/utils/cache/plancache.c:1103-1167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1103-L1167)

## Overview
cached_plan_cost calculates the estimated execution cost of a cached plan, optionally including planning overhead costs for accurate comparison between generic and custom plans.

## Definition


## Detailed Description
cached_plan_cost provides cost estimation for cached plans in PostgreSQL's plan cache system. It sums the total execution costs of all non-utility statements in the plan and optionally adds an estimate for planning costs when evaluating custom plans. The planning cost estimation uses a simple heuristic based on the number of relations in the plan's range table, multiplied by a fixed factor to approximate the computational effort required for query planning.

The function is crucial for the adaptive planning system's cost-based decisions between generic and custom plans. When include_planner is true, it adds planning overhead to reflect the true cost of custom plans, enabling fair comparison with generic plans that amortize planning costs across multiple executions.

## Parameters / Member Variables
- : The CachedPlan whose cost should be calculated
- : Whether to include estimated planning costs in the total

## Dependencies
- Functions called/Symbols referenced:
  - [PlannedStmt](../P/PlannedStmt.md) (for accessing individual statement plans)
  - CMD_UTILITY (to identify and skip utility statements)
  - list_length (to count relations in range table)
  - cpu_operator_cost (PostgreSQL cost parameter for CPU operations)
  - lfirst_node (list iteration macro)
- Called from (representative examples):
  - [GetCachedPlan](../G/GetCachedPlan.md)
  - StmtPlanRequiresRevalidation

## Notes and Other Information
- Utility statements (CMD_UTILITY) are ignored as they have no meaningful execution cost
- Planning cost estimation is admittedly crude, using a linear relationship with the number of relations
- The planning cost multiplier (1000 * cpu_operator_cost) is conservative and may underestimate actual planning effort
- Planning cost scaling doesn't account for join collapse limits or inheritance child relations
- The function acknowledges that join planning effort scales worse than linearly but uses linear estimation for simplicity
- Future improvements to cost estimation should be implemented in src/backend/optimizer/
- The total_cost field from PlannedStmt->planTree provides the base execution cost estimate
- This function is central to the cost-based decision making in choose_custom_plan