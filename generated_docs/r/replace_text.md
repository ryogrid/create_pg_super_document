# replace_text

## Location
[src/backend/utils/adt/varlena.c:3996-4072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3996-L4072)

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
  - [text_position_setup](../t/text_position_setup.md) (initialize text search)
  - [text_position_next](../t/text_position_next.md) (find next match)
  - [text_position_get_match_ptr](../t/text_position_get_match_ptr.md) (get match location)
  - [text_position_cleanup](../t/text_position_cleanup.md) (cleanup search state)
  - PG_GET_COLLATION (get current collation)
  - [initStringInfo](../i/initStringInfo.md) (initialize result buffer)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md) (append binary data)
  - [appendStringInfoText](../a/appendStringInfoText.md) (append text efficiently)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md) (convert result to text)
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

## Simplified Source

```c
Datum replace_text(PG_FUNCTION_ARGS) {
    text *src_text = PG_GETARG_TEXT_PP(0);
    text *from_sub_text = PG_GETARG_TEXT_PP(1);
    text *to_sub_text = PG_GETARG_TEXT_PP(2);

    int src_text_len = VARSIZE_ANY_EXHDR(src_text);
    int from_sub_text_len = VARSIZE_ANY_EXHDR(from_sub_text);

    // Return original string if empty source or pattern
    if (src_text_len < 1 || from_sub_text_len < 1) {
        PG_RETURN_TEXT_P(src_text);
    }

    // Set up text position search
    TextPositionState state;
    text_position_setup(src_text, from_sub_text, PG_GET_COLLATION(), &state);

    // Check if pattern exists
    bool found = text_position_next(&state);
    if (!found) {
        text_position_cleanup(&state);
        PG_RETURN_TEXT_P(src_text);
    }

    // Build result string with replacements
    StringInfoData str;
    initStringInfo(&str);

    char *start_ptr = VARDATA_ANY(src_text);
    char *curr_ptr;

    do {
        CHECK_FOR_INTERRUPTS();

        curr_ptr = text_position_get_match_ptr(&state);

        // Copy text before match
        int chunk_len = curr_ptr - start_ptr;
        appendBinaryStringInfo(&str, start_ptr, chunk_len);

        // Append replacement text
        appendStringInfoText(&str, to_sub_text);

        // Move past the matched pattern
        start_ptr = curr_ptr + from_sub_text_len;

        // Find next match
        found = text_position_next(&state);
    } while (found);

    // Copy remaining text
    int final_chunk_len = ((char *) src_text + VARSIZE_ANY(src_text)) - start_ptr;
    appendBinaryStringInfo(&str, start_ptr, final_chunk_len);

    text_position_cleanup(&state);

    // Convert result to text and cleanup
    text *ret_text = cstring_to_text_with_len(str.data, str.len);
    pfree(str.data);

    PG_RETURN_TEXT_P(ret_text);
}
```