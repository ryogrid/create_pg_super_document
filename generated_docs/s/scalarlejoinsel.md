# scalarlejoinsel

## Location
[src/backend/utils/adt/selfuncs.c:2910-2918](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L2910-L2918)

## Overview
Provides a default selectivity estimate for "<=" (less than or equal) join operations on scalar data types.

## Definition

```c
Datum
scalarlejoinsel(PG_FUNCTION_ARGS)
```
## Detailed Description
The `scalarlejoinsel` function is a simple selectivity estimator for scalar less-than-or-equal join operations. Like its counterpart `scalarltjoinsel`, it returns a constant default selectivity value defined by `DEFAULT_INEQ_SEL` without performing any statistical analysis of the actual data.

This function serves as a basic fallback estimator in PostgreSQL's cost-based query optimizer when more sophisticated selectivity estimation is not available or implemented for the specific scalar data types being joined. The constant approach provides a reasonable baseline estimate for query planning decisions.

## Parameters / Member Variables
- Standard PostgreSQL function arguments (`PG_FUNCTION_ARGS`) including:
  - `root`: PlannerInfo pointer (not explicitly used)
  - `operator`: OID of the less-than-or-equal operator (not explicitly used)
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
- Provides consistent behavior with other scalar inequality join selectivity estimators
- Location: src/backend/utils/adt/selfuncs.c:2910-2918

## Simplified Source

```c
// Simple selectivity estimator for scalar "<=" join operations
Datum scalarlejoinsel(PG_FUNCTION_ARGS) {
    // Returns default inequality selectivity constant
    // Ignores all input parameters for simplicity
    PG_RETURN_FLOAT8(DEFAULT_INEQ_SEL);
}
```