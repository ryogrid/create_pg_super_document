# scalarltjoinsel

## Location
src/backend/utils/adt/selfuncs.c: 2901 - 2909

## Overview
Provides a default selectivity estimate for "<" (less than) join operations on scalar data types.

## Definition


## Detailed Description
The `scalarltjoinsel` function is a simple selectivity estimator for scalar less-than join operations. Rather than performing complex statistical analysis, it returns a constant default selectivity value defined by `DEFAULT_INEQ_SEL`. This approach is used when more sophisticated selectivity estimation methods are not available or appropriate for the specific data types involved.

This function serves as a fallback estimator in PostgreSQL's query optimization framework, providing a reasonable baseline estimate for inequality joins involving scalar types when detailed statistics are unavailable.

## Parameters / Member Variables
- Standard PostgreSQL function arguments (`PG_FUNCTION_ARGS`) including:
  - `root`: PlannerInfo pointer (not explicitly used)
  - `operator`: OID of the less-than operator (not explicitly used)
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
- Likely used as a placeholder or fallback when more sophisticated estimation isn't implemented
- Location: src/backend/utils/adt/selfuncs.c:2901-2909