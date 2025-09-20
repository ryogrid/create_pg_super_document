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
- : Input text as a Datum (may be compressed/toasted)
- : Starting position (1-based indexing, can be zero or negative)
- : Number of characters to extract (can be negative to indicate error)
- : Boolean flag indicating if length should extend to end of string

## Dependencies
- Functions called/Symbols referenced:
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md)
  - cstring_to_text
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