# boolout

## Location
src/backend/utils/adt/bool.c: 157 - 173

## Overview
PostgreSQL output function for the boolean data type that converts internal boolean values to their string representation ("t" or "f").

## Definition


## Detailed Description
The `boolout` function serves as the output conversion function for PostgreSQL's boolean data type. It is automatically called by the PostgreSQL type system when converting internal boolean values to string representations for display or transmission. The function implements a simple and efficient conversion: true values become "t" and false values become "f". It allocates a minimal 2-character string (including null terminator) using PostgreSQL's memory management system and returns it as a C string.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro to access function call context
- Input parameter accessed via `PG_GETARG_BOOL(0)`: The internal boolean value to convert to string

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL (PostgreSQL function argument extraction macro for boolean)
  - palloc (PostgreSQL memory allocation function)
  - PG_RETURN_CSTRING (PostgreSQL return value macro for C strings)
- Called from (representative examples):
  - ExecGetJsonValueItemString (execExprInterp.c:4506)
  - PostgreSQL type system (no direct references in indexed code)

## Notes and Other Information
- This is a PostgreSQL "output function" registered in the system catalogs for the boolean data type
- Automatically invoked when PostgreSQL needs to convert boolean to text (e.g., SELECT, display, client communication)
- Uses PostgreSQL's shortest canonical representation: "t" for true, "f" for false
- Memory allocation uses `palloc` which is automatically freed by PostgreSQL's memory context system
- The function signature follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS
- Very lightweight implementation optimized for performance with minimal memory usage
- The output format is consistent with PostgreSQL's traditional boolean representation style