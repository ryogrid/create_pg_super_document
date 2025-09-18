# DatumGetNumeric

## Location
src/include/utils/numeric.h: 61 - 66

## Overview
DatumGetNumeric is an inline function that converts a Datum value to a Numeric pointer, properly handling TOAST decompression for numeric data types.

## Definition


## Detailed Description
This function serves as a conversion utility in PostgreSQL's function manager (fmgr) interface to extract Numeric data from a Datum. It wraps the PG_DETOAST_DATUM macro to ensure that if the numeric value is stored in TOAST format (for large values), it gets properly detoasted before being cast to a Numeric pointer. This is essential for accessing numeric values that may be stored externally due to PostgreSQL's TOAST (The Oversized Attribute Storage Technique) mechanism.

## Parameters / Member Variables
- : A Datum value containing numeric data that needs to be converted to a Numeric pointer

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM (macro for handling TOAST decompression)
  - Numeric (data type)
- Called from (representative examples):
  - extract_date
  - numeric_absolute
  - numeric_half_rounded
  - numeric_truncated_divide
  - pg_size_bytes
  - numeric_to_number
  - numeric_to_char
  - jsonb_in_scalar
  - datum_to_jsonb_internal
  - numeric_fast_cmp
  - timestamp_part_common
  - PG_GETARG_NUMERIC

## Notes and Other Information
- This is an inline function defined in src/include/utils/numeric.h for performance optimization
- Part of the fmgr interface macros used throughout PostgreSQL for type conversions
- Handles both regular and TOAST-compressed numeric values transparently
- Essential for any function that needs to work with Numeric data passed as Datum values
- The detoasting ensures that large numeric values stored externally are properly accessible