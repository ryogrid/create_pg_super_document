# boolge

## Location
[src/backend/utils/adt/bool.c:268-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L268-L286)

## Overview
The boolge function implements the greater-than-or-equal-to comparison operator (>=) for Boolean values in PostgreSQL, returning true if the first Boolean argument is greater than or equal to the second.

## Definition
```c
Datum boolge(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the implementation for the Boolean greater-than-or-equal-to operator in PostgreSQL's type system. It follows PostgreSQL's Boolean ordering where false < true, so the function returns true when the first argument is greater than or equal to the second argument. This means it returns true in three cases: when both arguments are false, when both arguments are true, or when the first argument is true and the second is false.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro which provides access to function arguments
- Argument 0: First Boolean value to compare
- Argument 1: Second Boolean value to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL (macro to extract Boolean arguments)
  - PG_RETURN_BOOL (macro to return Boolean result)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- Located in src/backend/utils/adt/bool.c:268-286
- Part of PostgreSQL's Boolean data type implementation
- Boolean comparison follows the ordering: false < true
- Returns true when arg1 >= arg2 (false >= false, true >= false, true >= true)
- Returns false only when arg1 is false and arg2 is true
- Integrated into PostgreSQL's operator system for the '>=' operator on Boolean types