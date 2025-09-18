# like_escape_bytea

## Location
src/backend/utils/adt/like.c: 447 - 454

## Overview
The `like_escape_bytea` function is a PostgreSQL SQL function that processes BYTEA (binary data) patterns for LIKE operations by converting user-specified escape characters to PostgreSQL's standard backslash escape convention.

## Definition
```c
Datum like_escape_bytea(PG_FUNCTION_ARGS)
```

## Detailed Description
The `like_escape_bytea` function is a wrapper function that provides escape character processing specifically for BYTEA data types in LIKE pattern matching operations. It takes two BYTEA arguments: a pattern and an escape string, then calls the underlying `SB_do_like_escape` function to perform the actual escape character conversion.

The function serves as a SQL-callable interface for the LIKE ESCAPE functionality on binary data. It converts patterns that use a user-specified escape character into PostgreSQL's internal standard format that uses backslash (\) as the escape character. This standardization allows the LIKE matching engine to work consistently regardless of what escape character the user originally specified.

The function handles the conversion by:
1. Accepting a BYTEA pattern and a BYTEA escape character specification
2. Casting the BYTEA types to text for processing by the shared escape logic
3. Converting the result back to BYTEA format for return

## Parameters / Member Variables
- `pat`: BYTEA pattern string that may contain wildcard characters (% and _) and escape sequences
- `esc`: BYTEA string specifying the escape character to be converted to backslash format

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BYTEA_PP` (macro for extracting BYTEA arguments from function call)
  - `PG_RETURN_BYTEA_P` (macro for returning BYTEA results)  
  - `SB_do_like_escape` (single-byte escape processing function from like_match.c)
- Called from (representative examples):
  - No direct references found in the analyzed codebase (likely called via SQL function dispatch)

## Notes and Other Information
- This function is part of PostgreSQL's LIKE pattern matching system for binary data types
- The function reuses the text-based escape logic by casting BYTEA to text, which works because the escape processing operates at the byte level
- The underlying `SB_do_like_escape` function handles various escape scenarios:
  - Empty escape string: doubles any backslashes in the pattern
  - Backslash escape: returns pattern unchanged  
  - Other escape characters: converts them to backslashes and handles backslash doubling
- The function is located in src/backend/utils/adt/like.c:447-454
- It's designed to work with PostgreSQL's function call interface using the PG_FUNCTION_ARGS macro system