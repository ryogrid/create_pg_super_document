# char_text

## Location
[src/backend/utils/adt/char.c:228-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/char.c#L228-L254)

## Overview
Converts a PostgreSQL "char" (single byte character) data type to text with proper handling of special characters and null bytes.

## Definition
```c
Datum char_text(PG_FUNCTION_ARGS)
```

## Detailed Description
This function converts a PostgreSQL "char" value to a text data type following specific conversion rules similar to charout(). It handles three main cases: (1) If the character has the high bit set (values >= 128), it converts the character to an octal escape sequence in the format \nnn. (2) If the character is not null ('\0'), it creates a single-character text value. (3) If the character is null, it creates an empty text string. The function dynamically allocates memory for the result text and sets the appropriate variable-length header information.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro to access function arguments
- `arg1`: The input "char" value retrieved using PG_GETARG_CHAR(0)
- `result`: Pointer to the allocated text structure to hold the converted result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CHAR (macro for extracting char argument)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - VARHDRSZ (constant for variable-length header size)
  - IS_HIGHBIT_SET (macro to check if high bit is set)
  - SET_VARSIZE (macro to set variable-length data size)
  - VARDATA (macro to access variable-length data content)
  - TOOCTAL (macro to convert numeric value to octal character)
  - PG_RETURN_TEXT_P (macro for returning text result)

- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL CAST operations)

## Notes and Other Information
- Handles high-bit characters by converting them to octal escape sequences (\nnn format)
- Converts null character ('\0') to empty text string for honest representation
- Uses dynamic memory allocation with palloc() for result storage
- The octal conversion uses bit shifting: first digit from bits 6-7, second from bits 3-5, third from bits 0-2
- Properly manages PostgreSQL's variable-length data structures with appropriate header information
- Used internally by PostgreSQL's type conversion system for char to text casts
- The function follows PostgreSQL's V1 calling convention using the PG_FUNCTION_ARGS interface