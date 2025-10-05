# like_escape

## Location
[src/backend/utils/adt/like.c:428-446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like.c#L428-L446)

## Overview
A PostgreSQL function that converts LIKE patterns with custom ESCAPE characters to PostgreSQL's standard backslash escape convention.

## Definition
```c
Datum like_escape(PG_FUNCTION_ARGS)
```

## Detailed Description
The `like_escape` function processes LIKE patterns that use custom escape characters and converts them to use PostgreSQL's standard backslash escape convention. This function is essential for handling SQL patterns that specify an ESCAPE clause, such as `LIKE 'pattern' ESCAPE 'character'`. 

The function determines whether to use single-byte or multi-byte processing based on the database encoding. For single-byte encodings (where each character is exactly one byte), it uses `SB_do_like_escape`, while for multi-byte encodings it uses `MB_do_like_escape`. This encoding-aware approach ensures proper handling of escape sequences across different character sets and locales.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: The original LIKE pattern text containing custom escape sequences
- `PG_GETARG_TEXT_PP(1)`: The custom escape character specification

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP` - Extract text arguments from function call
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md) - Determine if database uses single-byte or multi-byte encoding
  - `SB_do_like_escape` - Handle escape conversion for single-byte encodings
  - `MB_do_like_escape` - Handle escape conversion for multi-byte encodings
  - `PG_RETURN_TEXT_P` - Return text result
- Called from: 
  - This function is called through PostgreSQL's function manager when LIKE patterns with ESCAPE clauses are processed

## Notes and Other Information
- Essential for SQL standard compliance with ESCAPE clause support
- Automatically chooses appropriate processing method based on database encoding
- Converts custom escape characters to PostgreSQL's standard backslash convention
- Part of PostgreSQL's LIKE pattern processing infrastructure
- Located in src/backend/utils/adt/like.c:428-446
- The encoding check ensures optimal performance and correctness across different character sets
- Handles the transformation needed to standardize escape sequences before pattern matching

## Simplified Source

```c
Datum like_escape(PG_FUNCTION_ARGS) {
    text *pat = PG_GETARG_TEXT_PP(0);  // Original pattern with custom escape
    text *esc = PG_GETARG_TEXT_PP(1);  // Custom escape character
    text *result;

    // Choose processing method based on database encoding
    if (pg_database_encoding_max_length() == 1)
        result = SB_do_like_escape(pat, esc);  // Single-byte encoding
    else
        result = MB_do_like_escape(pat, esc);  // Multi-byte encoding

    PG_RETURN_TEXT_P(result);
}
```