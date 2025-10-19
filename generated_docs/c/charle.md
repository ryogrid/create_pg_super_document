# charle

## Location
[src/backend/utils/adt/char.c:154-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/char.c#L154-L162)

## Overview
The `charle` function implements the "less than or equal" comparison operation for PostgreSQL's single-byte character (`char`) data type.

## Definition
```c
Datum charle(PG_FUNCTION_ARGS)
```

## Detailed Description
This function compares two single-byte character values and returns true if the first character is less than or equal to the second character, false otherwise. It follows PostgreSQL's standard function calling convention using the `PG_FUNCTION_ARGS` macro and returns a `Datum` value. The function performs unsigned byte comparison by casting both arguments to `uint8` before comparison to ensure proper ordering.

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
- This function implements the SQL operator `<=` (less than or equal) for the `char` data type
- Located in `src/backend/utils/adt/char.c` at lines 154-162
- Part of PostgreSQL's built-in operator functions for character data types
- Uses `uint8` casting to ensure unsigned comparison semantics, treating characters as byte values 0-255
- This ensures consistent ordering regardless of whether `char` is signed or unsigned on the platform

## Simplified Source

```c
Datum charle(PG_FUNCTION_ARGS) {
    char arg1 = PG_GETARG_CHAR(0);
    char arg2 = PG_GETARG_CHAR(1);

    // Compare characters as unsigned bytes (less than or equal)
    PG_RETURN_BOOL((uint8) arg1 <= (uint8) arg2);
}
```