# replace_text

## Location
src/backend/utils/adt/varlena.c: 3996 - 4072

## Overview
The replace_text function implements the SQL REPLACE() function, replacing all occurrences of a specified substring with a replacement string in a text value.

## Definition
```c
Datum replace_text(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs string replacement by finding all occurrences of a pattern substring within a source text and replacing them with a specified replacement text. It uses PostgreSQL's text positioning infrastructure to efficiently locate matches while respecting collation rules for text comparison.

The implementation is optimized to handle multiple replacements in a single pass through the source string. It builds the result incrementally using a StringInfo buffer, copying unchanged portions and inserting replacement text where matches are found. The function respects the current collation setting for pattern matching, making it locale-aware when appropriate.

Key behaviors:
- Returns the original string unchanged if either source or pattern is empty
- Handles overlapping patterns correctly by advancing past each match
- Uses collation-aware text searching for proper internationalization support
- Efficiently manages memory through StringInfo and proper cleanup

## Parameters / Member Variables
- `src_text`: The source text to perform replacements on
- `from_sub_text`: The substring pattern to search for and replace
- `to_sub_text`: The replacement text to substitute for each match

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (argument extraction)
  - VARSIZE_ANY_EXHDR (get text length)
  - text_position_setup (initialize text search)
  - text_position_next (find next match)
  - text_position_get_match_ptr (get match location)
  - text_position_cleanup (cleanup search state)
  - PG_GET_COLLATION (get current collation)
  - initStringInfo (initialize result buffer)
  - appendBinaryStringInfo (append binary data)
  - appendStringInfoText (append text efficiently)
  - cstring_to_text_with_len (convert result to text)
  - CHECK_FOR_INTERRUPTS (allow query cancellation)
  - PG_RETURN_TEXT_P (return text result)
- Called from (representative examples):
  - SQL REPLACE() function calls
  - Extension script processing (execute_extension_script)
  - Regular expression replacement functions
  - Text manipulation utilities

## Notes and Other Information
- Implements the standard SQL REPLACE() function semantics
- Uses collation-aware text searching for proper internationalization
- Optimized for multiple replacements with single-pass algorithm
- Handles edge cases like empty strings and no matches gracefully
- Critical component of PostgreSQL's text manipulation capabilities
- Supports interruption for long-running operations on large texts