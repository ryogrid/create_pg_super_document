# adjust_limit_rows_costs

## Location
[src/backend/optimizer/util/pathnode.c:3881-3948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3881-L3948)

## Overview
Adjusts row count and cost estimates for a LimitPath node according to OFFSET and LIMIT clauses, providing accurate cost estimation for query planning when result sets are truncated.

## Definition

```c
void
adjust_limit_rows_costs(double *rows,	/* in/out parameter */
						Cost *startup_cost, /* in/out parameter */
						Cost *total_cost,	/* in/out parameter */
						int64 offset_est,
						int64 count_est)
```
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

## Simplified Source

```c
void
adjust_limit_rows_costs(double *rows, Cost *startup_cost, Cost *total_cost,
                        int64 offset_est, int64 count_est)
{
    double input_rows = *rows;
    Cost input_startup_cost = *startup_cost;
    Cost input_total_cost = *total_cost;

    // Handle OFFSET: increase startup cost, reduce row count
    if (offset_est != 0) {
        double offset_rows;

        // Use actual estimate or 10% heuristic
        if (offset_est > 0)
            offset_rows = (double) offset_est;
        else
            offset_rows = clamp_row_est(input_rows * 0.10);

        if (offset_rows > *rows)
            offset_rows = *rows;

        // Add cost for skipping offset_rows
        if (input_rows > 0)
            *startup_cost += (input_total_cost - input_startup_cost) *
                           offset_rows / input_rows;

        *rows -= offset_rows;
        if (*rows < 1)
            *rows = 1;
    }

    // Handle LIMIT: reduce total cost and row count
    if (count_est != 0) {
        double count_rows;

        // Use actual estimate or 10% heuristic
        if (count_est > 0)
            count_rows = (double) count_est;
        else
            count_rows = clamp_row_est(input_rows * 0.10);

        if (count_rows > *rows)
            count_rows = *rows;

        // Adjust total cost for limited output
        if (input_rows > 0)
            *total_cost = *startup_cost +
                         (input_total_cost - input_startup_cost) *
                         count_rows / input_rows;

        *rows = count_rows;
        if (*rows < 1)
            *rows = 1;
    }
}
```