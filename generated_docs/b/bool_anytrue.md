# bool_anytrue

## Location
[src/backend/utils/adt/bool.c:385-397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L385-L397)

## Overview
Final function for the SOME/ANY boolean aggregate, returning true if at least one non-null input value was true.

## Definition
```c
Datum bool_anytrue(PG_FUNCTION_ARGS)
```

## Detailed Description  
The bool_anytrue function serves as the final function for PostgreSQL's SOME/ANY and BOOL_OR aggregate functions. It examines the accumulated state to determine if at least one non-null boolean value in the aggregation was true. The function returns NULL if no non-null values were processed (following SQL standard semantics), and returns true whenever the count of true values is greater than zero. This implements the mathematical logic of existential quantification over a set of boolean values.

## Parameters / Member Variables
- First parameter (PG_GETARG_POINTER(0)): BoolAggState pointer containing accumulation results, may be NULL

## Dependencies
- Functions called/Symbols referenced:
  - [BoolAggState](../B/BoolAggState.md)
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_RETURN_NULL
  - PG_RETURN_BOOL
- Called from (representative examples):
  - PostgreSQL aggregate system for SOME/ANY/BOOL_OR operations

## Notes and Other Information
This function implements the SQL SOME/ANY aggregate semantics where the result is true if at least one input value is true, false if all input values are false, and NULL if all input values are NULL. The comparison (state->aggtrue > 0) efficiently determines if any non-null values were true by checking if the true count is positive. This function is typically registered in pg_proc as the final function for boolean OR aggregates and complements bool_alltrue in providing complete boolean aggregate functionality.

## Simplified Source
```c
bool bool_anytrue(BoolAggState* state) {
    // If no values were processed, return NULL
    if (state == NULL || state->aggcount == 0) {
        return NULL;
    }

    // Return true if at least one non-null value was true
    // (count of true values is greater than zero)
    return state->aggtrue > 0;
}
```