# scalargejoinsel

## Location
[src/backend/utils/adt/selfuncs.c:2928-2955](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L2928-L2955)

## Overview
Provides a default selectivity estimate for ">=" (greater than or equal) join operations on scalar data types.

## Definition


## Detailed Description
The `scalargejoinsel` function is a simple selectivity estimator for scalar greater-than-or-equal join operations. It completes the set of basic inequality join selectivity estimators by returning the same constant default selectivity value `DEFAULT_INEQ_SEL` used by its companion functions for other inequality operators.

This function maintains the uniform approach to selectivity estimation across all scalar inequality join operations in PostgreSQL. When the query optimizer encounters a >= join condition involving scalar types and lacks detailed statistical information, this function provides a consistent baseline estimate that enables reasonable query planning decisions.

## Parameters / Member Variables
- Standard PostgreSQL function arguments (`PG_FUNCTION_ARGS`) including:
  - `root`: PlannerInfo pointer (not explicitly used)
  - `operator`: OID of the greater-than-or-equal operator (not explicitly used)
  - `args`: List of join arguments (not explicitly used)
  - `jointype`: Type of join operation (not explicitly used)
  - `sjinfo`: SpecialJoinInfo structure (not explicitly used)

## Dependencies
- Functions called/Symbols referenced:
  - DEFAULT_INEQ_SEL (constant for default inequality selectivity)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Extremely simple implementation that ignores all input parameters
- Uses PostgreSQL's function manager interface (PG_FUNCTION_ARGS, PG_RETURN_FLOAT8)
- Part of the selectivity estimation framework in src/backend/utils/adt/selfuncs.c
- Completes the set of scalar inequality join selectivity functions (<, <=, >, >=)
- Location: src/backend/utils/adt/selfuncs.c:2928-2955