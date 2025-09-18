# text_substring

## Location
src/backend/utils/adt/varlena.c: 885 - 1092

## Overview
A PostgreSQL internal function that performs substring extraction from text data with optimized handling for different character encodings and compressed/toasted values.

## Definition


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
  - pg_database_encoding_max_length
  - pg_add_s32_overflow
  - cstring_to_text
  - DatumGetTextPSlice
  - pg_mul_s32_overflow
  - VARATT_IS_COMPRESSED
  - VARATT_IS_EXTERNAL
  - pg_mbstrlen_with_len
  - pg_mblen
  - SET_VARSIZE
  - VARDATA
- Called from (representative examples):
  - text_substr
  - text_substr_no_len
  - text_overlay
  - text_starts_with
  - text_left
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