# adjust_limit_rows_costs

## Location
src/backend/optimizer/util/pathnode.c: 3881 - 3948

## Overview
Adjusts row count and cost estimates for a LimitPath node according to OFFSET and LIMIT clauses, providing accurate cost estimation for query planning when result sets are truncated.

## Definition


## Detailed Description
This function modifies row count and cost estimates to reflect the impact of LIMIT and OFFSET clauses on query execution. It's crucial for providing accurate estimates when building subqueries, as the outer planner needs correct information about the expected output.

The function handles two main scenarios:
1. OFFSET processing: Increases startup cost proportionally to account for rows that must be skipped
2. LIMIT processing: Reduces total cost and row count to reflect the truncated result set

When offset or count values cannot be estimated (indicated by negative values), the function uses a conservative estimate of 10% of the input rows, clamped to a reasonable range using clamp_row_est(). The function ensures row counts never drop below 1, maintaining validity for further planning operations.

## Parameters / Member Variables
- : Pointer to row count estimate (modified in place)
- : Pointer to startup cost estimate (modified in place)
- : Pointer to total cost estimate (modified in place)
- : Estimated OFFSET value (0 = not present, -1 = cannot estimate, >0 = actual estimate)
- : Estimated LIMIT value (0 = not present, -1 = cannot estimate, >0 = actual estimate)

## Dependencies
- Functions called/Symbols referenced:
  - [clamp_row_est](../c/clamp_row_est.md) (ensures row estimates are within reasonable bounds)
  - Cost (cost estimation data type)
- Called from (representative examples):
  - [create_limit_path](../c/create_limit_path.md) (src/backend/optimizer/util/pathnode.c:3856)

## Notes and Other Information
- Does not include evaluation costs of OFFSET/LIMIT expressions themselves, as these are typically trivial
- Uses 10% heuristic when actual offset/count values cannot be estimated
- Ensures row counts never drop below 1 to maintain planning validity
- Critical for accurate subquery costing where outer planner needs precise estimates
- Startup cost increases proportionally with OFFSET to account for skipped rows
- Total cost adjusts proportionally with LIMIT to reflect reduced processing