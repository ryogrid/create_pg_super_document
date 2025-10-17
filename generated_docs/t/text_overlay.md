# text_overlay

## Location
[src/backend/utils/adt/varlena.c:1116-1152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1116-L1152)

## Overview
A PostgreSQL internal function that performs the core text overlay operation by replacing a specified substring with new text through substring extraction and concatenation.

## Definition

```c
static text *
text_overlay(text *t1, text *t2, int sp, int sl)
```
## Detailed Description
This function implements the core logic for PostgreSQL's OVERLAY() operation. It works by:

1. Validating input parameters and checking for integer overflow conditions
2. Extracting the prefix substring (from start to before the replacement position)
3. Extracting the suffix substring (from after the replaced section to the end)
4. Concatenating prefix + replacement text + suffix to form the result

The function includes comprehensive error checking for edge cases like negative start positions and potential integer overflows when calculating positions. It follows the SQL standard's definition of OVERLAY() which treats negative start positions as an error condition.

The implementation leverages existing PostgreSQL text manipulation functions ( and ) to perform the actual string operations, ensuring consistency with other text processing functions.

## Parameters / Member Variables
- `*t1`: The original text string to be modified
- `*t2`: The replacement text to insert
- `sp`: The substring start position (1-based, must be positive)
- `sl`: The substring length to replace
## Dependencies
- Functions called/Symbols referenced:
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md)
  - [text_substring](text_substring.md)
  - [text_catenate](text_catenate.md)
  - ereport
  - [PointerGetDatum](../P/PointerGetDatum.md)
- Called from (representative examples):
  - [textoverlay](textoverlay.md)
  - [textoverlay_no_len](textoverlay_no_len.md)
  - DatumGetVarStringPP

## Notes and Other Information
- This is a static (internal) function not directly callable from SQL
- Implements comprehensive input validation including overflow detection
- Uses 1-based indexing consistent with SQL standard
- Throws specific error codes for different failure conditions (ERRCODE_SUBSTRING_ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
- Returns a freshly allocated text datum that caller must manage
- Part of PostgreSQL's variable-length data type infrastructure in varlena.c
- The function constructs the result through three-part concatenation: prefix + replacement + suffix

## Simplified Source
```c
static text *
text_overlay(text *original_text, text *replacement_text, int start_pos, int length)
{
    // Validate inputs - check for negative start position
    if (start_pos <= 0) {
        ereport(ERROR, "negative substring length not allowed");
    }

    // Check for integer overflow when calculating end position
    int end_pos;
    if (pg_add_s32_overflow(start_pos, length, &end_pos)) {
        ereport(ERROR, "integer out of range");
    }

    // Extract three parts: prefix, middle (to be replaced), suffix
    // Part 1: prefix - characters before replacement (1 to start_pos-1)
    text *prefix = text_substring(PointerGetDatum(original_text),
                                 1, start_pos - 1, false);

    // Part 3: suffix - characters after replacement (end_pos to end)
    text *suffix = text_substring(PointerGetDatum(original_text),
                                 end_pos, -1, true);

    // Concatenate: prefix + replacement + suffix
    text *result = text_catenate(prefix, replacement_text);
    result = text_catenate(result, suffix);

    return result;
}
```