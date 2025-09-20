# boolor_statefunc

## Location
[src/backend/utils/adt/bool.c:299-303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L299-L303)

## Overview
The boolor_statefunc function implements the state transition function for the Boolean OR aggregate (bool_or), performing logical OR operations between Boolean values during aggregate computation.

## Definition
```c
Datum boolor_statefunc(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the state transition function for PostgreSQL's Boolean OR aggregate function, specifically for the bool_or aggregate that corresponds to the SQL standard ANY/SOME aggregate. It takes two Boolean arguments and returns their logical OR result. During aggregate computation, this function is called repeatedly to accumulate the OR operation across all input values. The function is designed for plain aggregate mode only, not moving-aggregate mode. The aggregate is named bool_or rather than ANY/SOME due to parsing conflicts with those keywords.

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
- Located in src/backend/utils/adt/bool.c:299-303
- Part of PostgreSQL's Boolean aggregate implementation
- Implements the state function for bool_or aggregate conforming to SQL 2003 ANY/SOME semantics
- Returns true if either current state or new input is true
- Used in plain aggregate mode only, not in moving-aggregate mode
- The aggregate returns true if any input value is true, false if all input values are false
- Named bool_or instead of ANY/SOME due to SQL parsing conflicts with those keywords