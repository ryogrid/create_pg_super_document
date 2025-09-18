# charne

## Location
src/backend/utils/adt/char.c: 136 - 144

## Overview
The `charne` function implements the "not equal" comparison operation for PostgreSQL's single-byte character (`char`) data type.

## Definition
```c
Datum charne(PG_FUNCTION_ARGS)
```

## Detailed Description
This function compares two single-byte character values and returns true if they are not equal, false otherwise. It follows PostgreSQL's standard function calling convention using the `PG_FUNCTION_ARGS` macro and returns a `Datum` value. The function performs a simple inequality check between the two character arguments.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument: First character value (retrieved via `PG_GETARG_CHAR(0)`)
  - Second argument: Second character value (retrieved via `PG_GETARG_CHAR(1)`)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CHAR`: Extracts character arguments from function call context
  - `PG_RETURN_BOOL`: Returns boolean result as PostgreSQL Datum
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function implements the SQL operator `<>` (not equal) for the `char` data type
- Located in `src/backend/utils/adt/char.c` at lines 136-144
- Part of PostgreSQL's built-in operator functions for character data types
- Uses direct comparison without casting, unlike some other character comparison functions