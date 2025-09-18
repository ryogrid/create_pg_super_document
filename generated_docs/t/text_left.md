# text_left

## Location
src/backend/utils/adt/varlena.c: 5538 - 5561

## Overview
PostgreSQL built-in function that returns the first n characters of a string, with special handling for negative values to return all but the last |n| characters.

## Definition
```c
Datum text_left(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements PostgreSQL's left() SQL function for extracting characters from the beginning of a text string. It handles two scenarios: when n is positive, it returns the first n characters using text_substring; when n is negative, it calculates the position by subtracting |n| from the total string length and uses multibyte-aware character clipping functions to handle Unicode properly. The function is designed to work correctly with multibyte character encodings by using PostgreSQL's multibyte string length and character clipping utilities.

## Parameters / Member Variables
- First argument (index 0): Input text string
- Second argument (index 1): Number of characters to extract (n)
  - If n > 0: return first n characters
  - If n < 0: return all but last |n| characters

## Dependencies
- Functions called/Symbols referenced:
  - pg_mbstrlen_with_len
  - pg_mbcharcliplen
  - cstring_to_text_with_len
  - text_substring
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - (No direct references found - called via SQL function dispatch)

## Notes and Other Information
- Implements the SQL left(string, n) function
- Properly handles multibyte character encodings (UTF-8, etc.)
- For positive n values, delegates to text_substring for efficiency
- For negative n values, performs manual multibyte-aware character counting and clipping
- Uses VARDATA_ANY and VARSIZE_ANY_EXHDR macros for safe text data access
- Returns appropriate text datum using PostgreSQL's standard return conventions
- Part of the text/varchar data type implementation in PostgreSQL