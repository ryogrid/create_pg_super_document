# multirangesel

## Location
src/backend/utils/adt/multirangetypes_selfuncs.c: 137 - 290

## Overview
PostgreSQL selectivity estimation function for multirange operators that calculates the probability of rows matching multirange query conditions.

## Definition
```c
Datum multirangesel(PG_FUNCTION_ARGS)
```

## Detailed Description
This is the main entry point for multirange operator selectivity estimation in the PostgreSQL query planner. The function analyzes query conditions involving multirange operators and estimates what fraction of rows will match the condition.

The function performs several key steps:
1. Extracts the operator, arguments, and variable information from the planner context
2. Validates that the expression is in the form "variable op constant" or "constant op variable"
3. Handles NULL constants (returns 0.0 selectivity)
4. Commutes operators when the variable is on the right side
5. Handles special cases like element containment (@>) by converting elements to singleton multiranges
6. Promotes range constants to multiranges for mixed-type operations
7. Delegates to calc_multirangesel() for detailed statistical analysis or falls back to default estimates

## Parameters / Member Variables
- `root`: PlannerInfo pointer containing query planning context
- `operator`: OID of the multirange operator being estimated
- `args`: List of arguments to the operator (typically variable and constant)
- `varRelid`: Relation ID for the variable, or 0 if multiple relations

## Dependencies
- Functions called/Symbols referenced:
  - get_restriction_variable
  - default_multirange_selectivity
  - get_commutator
  - multirange_get_typcache
  - range_serialize
  - make_multirange
  - DatumGetRangeTypeP
  - DatumGetMultirangeTypeP
  - calc_multirangesel
  - ReleaseVariableStats
  - CLAMP_PROBABILITY

- Called from (representative examples):
  - PostgreSQL query planner during selectivity estimation phase

## Notes and Other Information
This function is registered in PostgreSQL's system catalogs as the selectivity estimator for multirange operators. It handles complex type conversions between elements, ranges, and multiranges to provide unified selectivity estimation. The function ensures all estimates are clamped to valid probability values [0.0, 1.0] before returning.