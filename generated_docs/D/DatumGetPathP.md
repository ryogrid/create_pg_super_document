# DatumGetPathP

## Location
src/include/utils/geo_decls.h: 202 - 206

## Overview
DatumGetPathP is an inline function that converts a PostgreSQL Datum value to a PATH pointer, handling TOAST decompression if necessary.

## Definition


## Detailed Description
This function serves as a type-safe wrapper for converting PostgreSQL Datum values to PATH geometry pointers. It uses the PG_DETOAST_DATUM macro to ensure that if the Datum contains a TOASTed (compressed/externally stored) PATH value, it will be properly decompressed before returning the pointer. This is essential for PostgreSQL's geometric data types handling where large objects may be stored externally or compressed.

## Parameters / Member Variables
- : The input Datum value that should contain a PATH geometry object

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM (macro for TOAST decompression)
  - PATH (geometric data type)
- Called from (representative examples):
  - PG_GETARG_PATH_P (macro for function argument extraction)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Part of PostgreSQL's geometric data type conversion utilities
- Handles TOAST decompression transparently, which is crucial for large geometric objects
- Used primarily in geometric functions that need to extract PATH arguments from function calls