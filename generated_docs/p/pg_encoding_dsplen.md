# pg_encoding_dsplen

## Location
src/common/wchar.c: 2176 - 2188

## Overview
Returns the display length (visual width) of a multibyte character for proper text formatting and alignment in the specified encoding.

## Definition
```c
int pg_encoding_dsplen(int encoding, const char *mbstr)
```

## Detailed Description
This function calculates the display length of a multibyte character, which represents how many character positions the character occupies when displayed on screen. This is crucial for text formatting, alignment, and layout calculations, especially in multibyte encodings where a single character might take up multiple bytes in memory but appear as one or more display columns.

The function uses the encoding-specific display length function from the `pg_wchar_table` array. If an invalid encoding is provided, it falls back to ASCII display length calculation. Display length can differ from byte length - for example, some Asian characters may occupy 2 display columns while taking 3-4 bytes in UTF-8 encoding.

## Parameters / Member Variables
- `encoding`: The character encoding identifier (e.g., UTF8, Latin1, etc.)
- `mbstr`: Pointer to the multibyte character string to analyze

## Dependencies
- Functions called/Symbols referenced:
  - `PG_VALID_ENCODING`: Macro to validate encoding identifier (checks if encoding is between 0 and _PG_LAST_ENCODING_)
  - `PG_SQL_ASCII`: Fallback encoding constant (value 0) used when invalid encoding is provided
  - `pg_wchar_table[].dsplen`: Encoding-specific display length function pointer

- Called from (representative examples):
  - [PQdsplen](../P/PQdsplen.md): Public libpq function that wraps this function for client applications
  - `MIN_RIGHT_CUT`: Used in protocol handling for result formatting (fe-protocol3.c)

## Notes and Other Information
- This function is defined in src/common/wchar.c:2176-2188
- Display length is essential for proper text alignment in terminals and GUI applications
- The function handles various character encodings including single-byte and multibyte encodings
- For invalid encodings, it safely falls back to ASCII display length calculation
- Used extensively in PostgreSQL's client-side result formatting and display functionality
- Part of the core character encoding infrastructure that supports international text handling