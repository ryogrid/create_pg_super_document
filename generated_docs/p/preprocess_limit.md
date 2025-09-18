# preprocess_limit

## Location
src/backend/optimizer/plan/planner.c: 2473 - 2657

## Overview
Pre-estimates LIMIT and OFFSET clause values and calculates an adjusted tuple fraction to guide query planning optimization decisions.

## Definition
```c
static double preprocess_limit(PlannerInfo *root, double tuple_fraction,
                             int64 *offset_est, int64 *count_est)
```

## Detailed Description
This function performs early analysis of LIMIT and OFFSET clauses to provide the query planner with estimates that help optimize the execution plan. The function:

1. **Estimates clause values**: Uses estimate_expression_value() to evaluate LIMIT and OFFSET expressions, particularly useful when dealing with parameters or simple expressions.

2. **Handles special cases**:
   - NULL LIMIT is treated as LIMIT ALL (no limit)
   - Negative or zero LIMIT is normalized to 1
   - NULL or negative OFFSET is treated as 0
   - Non-constant expressions default to -1 (unknown)

3. **Calculates tuple fraction**: Adjusts the input tuple_fraction based on the estimated values:
   - For LIMIT: reduces the fraction to reflect fewer rows needed
   - For OFFSET only: increases the fraction since more rows must be fetched
   - Uses heuristic of 10% when expressions cannot be estimated

4. **Combines limits**: When both caller-provided tuple_fraction and LIMIT/OFFSET exist, applies logic to determine the most restrictive constraint.

## Parameters
- `root`: PlannerInfo structure containing query parse tree and planning context
- `tuple_fraction`: Input fraction of tuples expected to be retrieved by caller
- `offset_est`: Output parameter for estimated OFFSET value (0=not present, -1=unknown)
- `count_est`: Output parameter for estimated LIMIT value (0=not present, -1=unknown)

## Dependencies
- Functions called/Symbols referenced:
  - [estimate_expression_value](../e/estimate_expression_value.md)
  - [DatumGetInt64](../D/DatumGetInt64.md)
  - IsA, Const node type checking
- Called from:
  - [grouping_planner](../g/grouping_planner.md)
  - standard_qp_extra

## Notes and Other Information
- Returns the adjusted tuple_fraction to guide subsequent planning phases
- The function enforces a minimum estimated count of 1 for LIMIT to align with planner conventions
- Handles complex interactions between absolute counts (≥1.0) and fractional estimates (<1.0)
- Critical for optimizing queries with pagination patterns
- Located in src/backend/optimizer/plan/planner.c:2473-2657