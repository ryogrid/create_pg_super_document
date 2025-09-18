# bool_alltrue

## Location
[src/backend/utils/adt/bool.c:370-384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L370-L384)

## Overview
Final function for the EVERY/ALL boolean aggregate, returning true only if all non-null input values were true.

## Definition
```c
Datum bool_alltrue(PG_FUNCTION_ARGS)
```

## Detailed Description
The bool_alltrue function serves as the final function for PostgreSQL's EVERY and BOOL_AND aggregate functions. It examines the accumulated state to determine if all non-null boolean values in the aggregation were true. The function returns NULL if no non-null values were processed (following SQL standard semantics), and returns true only when the count of true values equals the total count of non-null values. This implements the mathematical logic of universal quantification over a set of boolean values.

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
  - PostgreSQL aggregate system for EVERY/BOOL_AND operations

## Notes and Other Information
This function implements the SQL EVERY aggregate semantics where the result is true if and only if all input values are true, false if any input value is false, and NULL if all input values are NULL. The comparison (state->aggtrue == state->aggcount) efficiently determines if all non-null values were true without needing to track false values separately. This function is typically registered in pg_proc as the final function for boolean AND aggregates.