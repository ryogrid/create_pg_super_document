# bytea_overlay

## Location
src/backend/utils/adt/varlena.c: 3118 - 3150

## Overview
A static helper function that performs the core implementation of bytea overlay operations, replacing a specified substring with another bytea by using substring extraction and concatenation.

## Definition


## Detailed Description
The `bytea_overlay` function implements the core logic for the SQL OVERLAY() operation on bytea data types. It follows the SQL standard approach by breaking down the overlay operation into three parts: extracting the substring before the replacement point, the replacement string itself, and the substring after the replacement point, then concatenating them together. The function includes robust error checking for integer overflow conditions and validates that the start position is positive. The implementation uses the existing `bytea_substring` and `bytea_catenate` functions to perform the actual work.

## Parameters / Member Variables
- `bytea *t1`: The target bytea string to be modified
- `bytea *t2`: The replacement bytea string to insert
- `int sp`: The substring start position (1-based, must be positive)
- `int sl`: The length of the substring to replace

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting)
  - pg_add_s32_overflow (for safe integer addition)
  - bytea_substring (for extracting substrings before and after replacement point)
  - PointerGetDatum (for converting pointers to Datum)
  - bytea_catenate (for concatenating bytea strings)
- Called from:
  - byteaoverlay (four-argument variant)
  - byteaoverlay_no_len (three-argument variant)

## Notes and Other Information
- This is a static function, not exposed outside varlena.c
- Performs comprehensive error checking for negative start positions and integer overflow
- Uses 1-based indexing consistent with SQL standard
- The algorithm splits the original string into two parts around the replacement region
- Throws ERRCODE_SUBSTRING_ERROR for negative start positions
- Throws ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE for integer overflow conditions
- Located in src/backend/utils/adt/varlena.c:3118-3150