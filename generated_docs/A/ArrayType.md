# ArrayType

## Location
src/include/utils/array.h: 92 - 98

## Overview
ArrayType is a fundamental structure in PostgreSQL that represents array values as varlena objects, providing the core data structure for all array types in the system.

## Definition


## Detailed Description
ArrayType serves as the header structure for PostgreSQL array values. It follows the varlena convention where the first int32 contains the total object size in bytes. The structure is designed to handle multi-dimensional arrays with optional null bitmaps. When dataoffset is 0, there is no null bitmap and array data follows immediately after the header. When dataoffset is non-zero, it indicates the byte offset from the start of the ArrayType structure to where the actual array data begins, with a null bitmap stored in between.

## Parameters / Member Variables
- : The varlena header containing the total object size in bytes (must be accessed via VARSIZE() and SET_VARSIZE() macros)
- : The number of dimensions in the array
- : Byte offset to the actual array data, or 0 if there is no null bitmap
- : The OID of the element type stored in this array

## Dependencies
- Functions called/Symbols referenced:
  - Uses varlena conventions (VARSIZE, SET_VARSIZE macros)
  - Oid type for element type identification
- Called from (representative examples):
  - DatumGetArrayTypeP() - converts Datum to ArrayType pointer
  - DatumGetArrayTypePCopy() - gets a copy of ArrayType from Datum
  - Various array manipulation functions throughout the codebase

## Notes and Other Information
- This structure is also used as the basis for specialized array types like oidvector and int2vector
- The varlena header should never be accessed directly; always use the VARSIZE() and SET_VARSIZE() macros
- Changes to this structure require corresponding changes to oidvector and int2vector headers
- The actual array data (dimension bounds, null bitmap if present, and element values) follows this header structure