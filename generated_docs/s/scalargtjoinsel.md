# scalargtjoinsel

## Location
src/backend/utils/adt/selfuncs.c: 2919 - 2927

## Overview
Provides a default selectivity estimate for ">" (greater than) join operations on scalar data types.

## Definition


## Detailed Description
The `scalargtjoinsel` function is a simple selectivity estimator for scalar greater-than join operations. Following the same pattern as other scalar inequality join estimators in PostgreSQL, it returns the constant default selectivity value `DEFAULT_INEQ_SEL` without analyzing the actual data distribution or statistics.

This function provides a consistent fallback mechanism within PostgreSQL's query optimization framework when detailed statistical analysis is not available for the specific scalar data types involved in the greater-than join predicate. The uniform approach across inequality operators ensures predictable query planning behavior.

## Parameters / Member Variables
- Standard PostgreSQL function arguments (`PG_FUNCTION_ARGS`) including:
  - `root`: PlannerInfo pointer (not explicitly used)
  - `operator`: OID of the greater-than operator (not explicitly used)
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
- Maintains consistency with other scalar inequality join selectivity functions
- Location: src/backend/utils/adt/selfuncs.c:2919-2927