# preprocess_limit

## Location
[src/backend/optimizer/plan/planner.c:2473-2657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L2473-L2657)

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

## Simplified Source

```c
static double
preprocess_limit(PlannerInfo *root, double tuple_fraction,
                int64 *offset_est, int64 *count_est)
{
    Query *parse = root->parse;
    Node *est;
    double limit_fraction;

    // Must have LIMIT or OFFSET clause
    Assert(parse->limitCount || parse->limitOffset);

    // Estimate LIMIT value
    if (parse->limitCount) {
        est = estimate_expression_value(root, parse->limitCount);
        if (est && IsA(est, Const)) {
            if (((Const *) est)->constisnull) {
                *count_est = 0;  // NULL means no limit
            } else {
                *count_est = DatumGetInt64(((Const *) est)->constvalue);
                if (*count_est <= 0)
                    *count_est = 1;  // Minimum of 1
            }
        } else {
            *count_est = -1;  // Cannot estimate
        }
    } else {
        *count_est = 0;  // No LIMIT clause
    }

    // Estimate OFFSET value
    if (parse->limitOffset) {
        est = estimate_expression_value(root, parse->limitOffset);
        if (est && IsA(est, Const)) {
            if (((Const *) est)->constisnull) {
                *offset_est = 0;  // NULL means no offset
            } else {
                *offset_est = DatumGetInt64(((Const *) est)->constvalue);
                if (*offset_est < 0)
                    *offset_est = 0;  // Negative means no offset
            }
        } else {
            *offset_est = -1;  // Cannot estimate
        }
    } else {
        *offset_est = 0;  // No OFFSET clause
    }

    // Calculate adjusted tuple fraction
    if (*count_est != 0) {
        // LIMIT case: reduce fraction since fewer rows needed
        if (*count_est < 0 || *offset_est < 0) {
            limit_fraction = 0.10;  // Default estimate for expressions
        } else {
            limit_fraction = (double) *count_est + (double) *offset_est;
        }

        // Choose smaller constraint between caller and LIMIT
        if (tuple_fraction >= 1.0 && limit_fraction >= 1.0) {
            tuple_fraction = Min(tuple_fraction, limit_fraction);
        } else if (tuple_fraction > 0.0 && limit_fraction >= 1.0) {
            tuple_fraction = limit_fraction;
        } else if (tuple_fraction > 0.0) {
            tuple_fraction = Min(tuple_fraction, limit_fraction);
        } else {
            tuple_fraction = limit_fraction;
        }
    }
    else if (*offset_est != 0 && tuple_fraction > 0.0) {
        // OFFSET only: increase fraction since more rows must be fetched
        if (*offset_est < 0) {
            limit_fraction = 0.10;  // Default estimate
        } else {
            limit_fraction = (double) *offset_est;
        }

        // Add offset to existing fraction
        if (tuple_fraction >= 1.0 && limit_fraction >= 1.0) {
            tuple_fraction += limit_fraction;
        } else if (tuple_fraction < 1.0 && limit_fraction < 1.0) {
            tuple_fraction += limit_fraction;
            if (tuple_fraction >= 1.0)
                tuple_fraction = 0.0;  // Fetch all
        }
    }

    return tuple_fraction;
}
```