# booland_statefunc

## Location
[src/backend/utils/adt/bool.c:287-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L287-L298)

## Overview
The booland_statefunc function implements the state transition function for the Boolean AND aggregate (EVERY and bool_and), performing logical AND operations between Boolean values during aggregate computation.

## Definition
```c
Datum booland_statefunc(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the state transition function for PostgreSQL's Boolean AND aggregate functions, specifically for the SQL standard EVERY aggregate and the PostgreSQL-specific bool_and aggregate. It takes two Boolean arguments and returns their logical AND result. During aggregate computation, this function is called repeatedly to accumulate the AND operation across all input values. The function is designed for plain aggregate mode only, not moving-aggregate mode.

## Parameters / Member Variables
- Argument 0: Current aggregate state (Boolean value)
- Argument 1: Next input value to aggregate (Boolean value)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL (macro to extract Boolean arguments)
  - PG_RETURN_BOOL (macro to return Boolean result)
- Called from (representative examples):
  - No direct references found in the codebase (called through PostgreSQL's aggregate function dispatch system)

## Notes and Other Information
- Located in src/backend/utils/adt/bool.c:287-298
- Part of PostgreSQL's Boolean aggregate implementation
- Implements the state function for EVERY and bool_and aggregates conforming to SQL 2003
- Returns true only if both current state and new input are true
- Used in plain aggregate mode only, not in moving-aggregate mode
- The aggregate returns true if all input values are true, false if any input value is false