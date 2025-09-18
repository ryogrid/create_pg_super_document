# chargt

## Location
[src/backend/utils/adt/char.c:163-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/char.c#L163-L171)

## Overview
The `chargt` function implements the "greater than" comparison operation for PostgreSQL's single-byte character (`char`) data type.

## Definition
```c
Datum chargt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function compares two single-byte character values and returns true if the first character is greater than the second character, false otherwise. It follows PostgreSQL's standard function calling convention using the `PG_FUNCTION_ARGS` macro and returns a `Datum` value. The function performs unsigned byte comparison by casting both arguments to `uint8` before comparison to ensure proper ordering.

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
- This function implements the SQL operator `>` (greater than) for the `char` data type
- Located in `src/backend/utils/adt/char.c` at lines 163-171
- Part of PostgreSQL's built-in operator functions for character data types
- Uses `uint8` casting to ensure unsigned comparison semantics, treating characters as byte values 0-255
- This ensures consistent ordering regardless of whether `char` is signed or unsigned on the platform