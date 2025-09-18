# boolgt

## Location
src/backend/utils/adt/bool.c: 250 - 258

## Overview
The boolgt function implements the greater-than comparison operator (>) for Boolean values in PostgreSQL, returning true if the first Boolean argument is greater than the second.

## Definition


## Detailed Description
This function provides the implementation for the Boolean greater-than operator in PostgreSQL's type system. It follows PostgreSQL's Boolean ordering where false < true, so the function returns true only when the first argument is true and the second argument is false. The function is part of PostgreSQL's internal function framework and uses the standard PostgreSQL function calling conventions.

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
- Located in src/backend/utils/adt/bool.c:250-258
- Part of PostgreSQL's Boolean data type implementation
- Boolean comparison follows the ordering: false < true
- Returns true only when arg1 is true and arg2 is false
- Integrated into PostgreSQL's operator system for the '>' operator on Boolean types