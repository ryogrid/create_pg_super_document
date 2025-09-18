# text_substr

## Location
[src/backend/utils/adt/varlena.c:852-865](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L852-L865)

## Overview
Returns a substring from a text value starting at a specified position with a given length, following SQL standard behavior.

## Definition
```c
Datum text_substr(PG_FUNCTION_ARGS)
```

## Detailed Description
The `text_substr` function implements PostgreSQL's substring extraction functionality, providing a SQL-compatible interface for extracting portions of text values. This function serves as a wrapper around the internal `text_substring` function, which handles the complex logic for substring extraction.

The function has evolved significantly over PostgreSQL's history, with contributions from Thomas Lockhart (1997), Tatsuo Ishii (1998 - multibyte support), John Gray (2002 - TOAST-slicing interface), and Joe Conway (2002 - SQL compliance fixes). The current implementation uses the faster TOAST-slicing interface for improved performance with large text values.

The function follows SQL standard behavior for edge cases: if the starting position is zero or negative, it adjusts the extraction to start from the beginning of the string; if the length is negative, it returns the remaining portion of the string from the starting position.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: Source text datum from which to extract substring
  - Argument 1: Starting position (1-based, following SQL convention)
  - Argument 2: Length of substring to extract

## Dependencies
- Functions called/Symbols referenced:
  - [text_substring](text_substring.md): Internal function that performs the actual substring extraction
  - `PG_GETARG_DATUM`: Extracts the source text datum
  - `PG_GETARG_INT32`: Extracts integer arguments for position and length
  - `PG_RETURN_TEXT_P`: Returns the resulting text substring

- Called from (representative examples):
  - [textregexsubstr](textregexsubstr.md): Regular expression substring extraction
  - [build_regexp_match_result](../b/build_regexp_match_result.md): Building results from regexp matches
  - [build_regexp_split_result](../b/build_regexp_split_result.md): Building results from regexp splits
  - [regexp_substr](../r/regexp_substr.md): Regular expression-based substring function
  - [build_test_match_result](../b/build_test_match_result.md): Test module for regex functionality

## Notes and Other Information
- This function is typically called through SQL's SUBSTRING function
- Uses 1-based positioning following SQL standards (not 0-based like many programming languages)
- The extensive comment history shows the evolution of substring handling in PostgreSQL
- Supports multibyte character encodings correctly
- Uses the efficient TOAST-slicing interface for handling large text values
- The `false` parameter passed to `text_substring` indicates this is not a no-length variant
- Handles edge cases according to SQL specification rather than throwing errors
- Performance optimized for both small strings and large TOAST-ed values