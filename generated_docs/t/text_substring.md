# text_substring

## Location
[src/backend/utils/adt/varlena.c:885-1092](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L885-L1092)

## Overview
A PostgreSQL internal function that performs substring extraction from text data with optimized handling for different character encodings and compressed/toasted values.

## Definition

```c
static text *
text_substring(Datum str, int32 start, int32 length, bool length_not_specified)
```
## Detailed Description
This is the core implementation function that handles substring extraction for PostgreSQL text types. It serves as the backend for both  and  functions. The function is designed to handle various edge cases including:

- Different character encodings (single-byte vs multi-byte)
- Compressed and toasted (out-of-line) text data
- Negative start positions and lengths
- Start positions beyond string boundaries
- Automatic length calculation when not specified

The function implements SQL99 compliance for substring operations, including proper handling of zero/negative start positions and lengths. It optimizes performance by avoiding full detoasting when possible and using appropriate slicing strategies based on the database encoding's maximum character length.

## Parameters / Member Variables
- `str`: Input text as a Datum (may be compressed/toasted)
- `start`: Starting position (1-based indexing, can be zero or negative)
- `length`: Number of characters to extract (can be negative to indicate error)
- `length_not_specified`: Boolean flag indicating if length should extend to end of string
## Dependencies
- Functions called/Symbols referenced:
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md)
  - [cstring_to_text](../c/cstring_to_text.md)
  - DatumGetTextPSlice
  - [pg_mul_s32_overflow](../p/pg_mul_s32_overflow.md)
  - VARATT_IS_COMPRESSED
  - VARATT_IS_EXTERNAL
  - [pg_mbstrlen_with_len](../p/pg_mbstrlen_with_len.md)
  - [pg_mblen](../p/pg_mblen.md)
  - SET_VARSIZE
  - VARDATA
- Called from (representative examples):
  - [text_substr](text_substr.md)
  - [text_substr_no_len](text_substr_no_len.md)
  - [text_overlay](text_overlay.md)
  - [text_starts_with](text_starts_with.md)
  - [text_left](text_left.md)
  - DatumGetVarStringPP

## Notes and Other Information
- This is a static (internal) function not directly callable from SQL
- Implements different optimization strategies based on encoding max length (eml)
- For single-byte encodings (eml == 1), uses simple byte-based slicing
- For multi-byte encodings (eml > 1), performs character-aware processing
- Handles SQL99 compliance including error reporting for negative lengths
- Part of PostgreSQL's variable-length data type infrastructure
- Returns freshly palloc'd text datum that caller must manage
- Located in src/backend/utils/adt/varlena.c along with other varlena utilities

## Simplified Source
```c
static text *
text_substring(Datum str, int32 start, int32 length, bool length_not_specified)
{
    int32 eml = pg_database_encoding_max_length();
    int32 adjusted_start = Max(start, 1);  // SQL99: start position at least 1

    // Single-byte encoding: fast path
    if (eml == 1) {
        int32 adjusted_length;

        if (length_not_specified) {
            adjusted_length = -1;  // To end of string
        } else if (length < 0) {
            ereport(ERROR, "negative substring length not allowed");
        } else {
            int32 end_pos;
            if (pg_add_s32_overflow(start, length, &end_pos)) {
                adjusted_length = -1;  // Overflow, use rest of string
            } else if (end_pos < 1) {
                return cstring_to_text("");  // Empty result
            } else {
                adjusted_length = end_pos - adjusted_start;
            }
        }

        // Use efficient byte-based slicing
        return DatumGetTextPSlice(str, adjusted_start - 1, adjusted_length);
    }

    // Multi-byte encoding: character-aware processing
    else if (eml > 1) {
        // Get slice that might contain the desired substring
        text *slice = get_conservative_slice(str, start, length, eml);

        if (VARSIZE_ANY_EXHDR(slice) == 0) {
            cleanup_and_return_empty(slice, str);
        }

        // Calculate actual character positions in the slice
        int32 slice_char_len = pg_mbstrlen_with_len(VARDATA_ANY(slice),
                                                   VARSIZE_ANY_EXHDR(slice));

        if (adjusted_start > slice_char_len) {
            cleanup_and_return_empty(slice, str);
        }

        // Find byte positions for the substring
        char *start_ptr = find_char_position(slice, adjusted_start);
        char *end_ptr = find_end_position(start_ptr, adjusted_start, length, slice_char_len);

        // Create result text
        text *result = create_result_text(start_ptr, end_ptr - start_ptr);

        cleanup_slice_if_needed(slice, str);
        return result;
    }

    elog(ERROR, "invalid backend encoding: encoding max length < 1");
    return NULL;
}
```