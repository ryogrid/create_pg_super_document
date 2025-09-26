# pg_get_function_identity_arguments

## Location
[src/backend/utils/adt/ruleutils.c:3159-3183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L3159-L3183)

## Overview
Returns a formatted list of arguments for a function suitable for use in ALTER FUNCTION and similar statements, specifically excluding default parameter values.

## Definition
```c
Datum pg_get_function_identity_arguments(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL-callable function takes a function OID as input and returns a formatted text representation of the function's argument list without default values. This is specifically designed for use in ALTER FUNCTION statements and other contexts where the function identity needs to be specified without including parameter defaults. The function retrieves metadata from the pg_proc system catalog and formats only the essential argument information needed to uniquely identify the function.

## Parameters / Member Variables
- `funcid`: OID of the target function whose identity arguments are to be formatted

## Dependencies
- Functions called/Symbols referenced:
  - [print_function_arguments](print_function_arguments.md)
  - [string_to_text](../s/string_to_text.md)
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- Returns NULL if the function OID is invalid or the function doesn't exist
- Uses the print_function_arguments helper function with parameters (false, false) to exclude default values
- The key difference from pg_get_function_arguments is that this function omits parameter defaults
- Essential for generating ALTER FUNCTION statements where defaults should not be included
- Part of the ruleutils.c module for SQL statement formatting