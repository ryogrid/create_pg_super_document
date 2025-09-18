# NumericData

## Location
src/backend/utils/adt/numeric.c: 153 - 163

## Overview
NumericData is the top-level structure for PostgreSQL numeric values, providing the varlena header and containing the union of possible numeric storage formats.

## Definition
```c
struct NumericData
{
    int32       vl_len_;        /* varlena header (do not touch directly!) */
    union NumericChoice choice; /* choice of format */
};
```

## Detailed Description
NumericData serves as the complete representation of a PostgreSQL numeric value as stored on disk and in memory. It combines the PostgreSQL varlena header (which contains length and other metadata for variable-length data types) with a NumericChoice union that can hold any of the supported numeric formats (NumericShort, NumericLong, or special values like NaN/Infinity).

This structure represents the actual "Numeric" data type as seen by PostgreSQL's type system. The vl_len_ field follows PostgreSQL's standard varlena conventions for variable-length types, while the choice field contains the actual numeric data in one of several possible formats depending on the value's characteristics.

## Parameters / Member Variables
- `vl_len_`: Standard PostgreSQL varlena header containing length and metadata information (should not be accessed directly)
- `choice`: Union containing the actual numeric data in one of the supported formats (NumericShort, NumericLong, or special values)

## Dependencies
- Functions called/Symbols referenced:
  - NumericChoice (union of numeric storage formats)
- Called from (representative examples):
  - NUMERIC_MIN_SIG_DIGITS (minimum significant digits constant)
  - Numeric (type alias for NumericData pointer)

## Notes and Other Information
- This is the complete, top-level structure for all PostgreSQL numeric values
- Follows PostgreSQL's varlena conventions for variable-length data types
- The vl_len_ field should be manipulated using PostgreSQL's varlena macros, not directly
- The choice union automatically selects the appropriate storage format based on the numeric value
- Represents the actual on-disk and in-memory format for the SQL NUMERIC/DECIMAL data type
- Part of PostgreSQL's sophisticated numeric storage system that optimizes space usage