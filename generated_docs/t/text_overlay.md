# text_overlay

## Location
src/backend/utils/adt/varlena.c: 1116 - 1152

## Overview
A PostgreSQL internal function that performs the core text overlay operation by replacing a specified substring with new text through substring extraction and concatenation.

## Definition


## Detailed Description
This function implements the core logic for PostgreSQL's OVERLAY() operation. It works by:

1. Validating input parameters and checking for integer overflow conditions
2. Extracting the prefix substring (from start to before the replacement position)
3. Extracting the suffix substring (from after the replaced section to the end)
4. Concatenating prefix + replacement text + suffix to form the result

The function includes comprehensive error checking for edge cases like negative start positions and potential integer overflows when calculating positions. It follows the SQL standard's definition of OVERLAY() which treats negative start positions as an error condition.

The implementation leverages existing PostgreSQL text manipulation functions ( and ) to perform the actual string operations, ensuring consistency with other text processing functions.

## Parameters / Member Variables
- : The original text string to be modified
- : The replacement text to insert
- : The substring start position (1-based, must be positive)
- : The substring length to replace

## Dependencies
- Functions called/Symbols referenced:
  - pg_add_s32_overflow
  - text_substring
  - text_catenate
  - ereport
  - PointerGetDatum
- Called from (representative examples):
  - textoverlay
  - textoverlay_no_len
  - DatumGetVarStringPP

## Notes and Other Information
- This is a static (internal) function not directly callable from SQL
- Implements comprehensive input validation including overflow detection
- Uses 1-based indexing consistent with SQL standard
- Throws specific error codes for different failure conditions (ERRCODE_SUBSTRING_ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
- Returns a freshly allocated text datum that caller must manage
- Part of PostgreSQL's variable-length data type infrastructure in varlena.c
- The function constructs the result through three-part concatenation: prefix + replacement + suffix