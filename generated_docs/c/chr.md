# chr

## Location
src/backend/utils/adt/oracle_compat.c: 1006 - 1120

## Overview
Converts an integer value to its corresponding character representation, supporting both ASCII and Unicode character sets depending on database encoding.

## Definition
```c
Datum chr(PG_FUNCTION_ARGS)
```

## Detailed Description
The chr function performs the inverse operation of the ascii function, converting a numeric value into its character representation. The function behavior depends on the database encoding: for UTF-8 databases, it treats the input as a Unicode code point and generates the appropriate multibyte UTF-8 sequence; for other multibyte encodings, it restricts input to ASCII range (1-127); for single-byte encodings, it accepts values 1-255. The function includes comprehensive validation to ensure the generated character is valid in the current database encoding, preventing invalid data from entering the database. It handles UTF-8 encoding with proper multibyte sequence generation for 2, 3, or 4-byte characters.

## Parameters / Member Variables
- `arg`: Integer value to be converted to a character (must be positive and non-zero)

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - PG_UTF8
  - SET_VARSIZE
  - VARDATA
  - [pg_utf8_islegal](../p/pg_utf8_islegal.md)
  - [pg_encoding_max_length](../p/pg_encoding_max_length.md)
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - Multiple references in regex engine (src/backend/regex/)
  - Color management functions in regex compiler
  - Character vector operations
  - Lexical analysis functions

## Notes and Other Information
- Located in src/backend/utils/adt/oracle_compat.c:1006-1120
- Part of PostgreSQL's Oracle compatibility layer
- Validates input parameters: rejects negative values and zero (null character)
- For UTF-8, implements full Unicode encoding up to U+10FFFF (RFC3629 limit)
- Generates proper 2, 3, or 4-byte UTF-8 sequences using bit manipulation
- Includes validation using pg_utf8_islegal to reject invalid sequences (e.g., surrogate pairs)
- For non-UTF-8 multibyte encodings, restricts to ASCII range for safety
- Ensures database encoding integrity by preventing invalid character data
- Memory allocation is optimized based on the expected output size