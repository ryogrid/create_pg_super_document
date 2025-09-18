# DatumGetMultirangeTypePCopy

## Location
src/include/utils/multirangetypes.h: 54 - 59

## Overview
DatumGetMultirangeTypePCopy is an inline function that converts a PostgreSQL Datum value to a MultirangeType pointer, ensuring the result is always a writable copy through TOAST decompression and copying.

## Definition
```c
static inline MultirangeType *
DatumGetMultirangeTypePCopy(Datum X)
```

## Detailed Description
This function serves as a type conversion utility for PostgreSQL's multirange data types, similar to DatumGetMultirangeTypeP but with an important distinction: it always returns a writable copy of the data. The function takes a Datum and converts it to a MultirangeType pointer, but unlike the non-copy version, it uses PG_DETOAST_DATUM_COPY which ensures that the returned pointer points to a newly allocated, modifiable copy of the data.

This is crucial when the caller needs to modify the multirange data, as the original Datum might point to read-only memory (such as data stored in tuples on disk or in shared buffers). The copy semantics ensure that any modifications won't affect the original data or cause memory corruption.

## Parameters / Member Variables
- `X`: A Datum value containing a multirange type that needs to be converted to a writable MultirangeType pointer

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM_COPY (macro for TOAST decompression with copying)
  - MultirangeType (return type)
- Called from (representative examples):
  - PG_GETARG_MULTIRANGE_P_COPY (macro for retrieving writable function arguments)

## Notes and Other Information
- Essential for operations that need to modify multirange data without affecting the original
- The copy semantics make this function slightly more expensive than DatumGetMultirangeTypeP but necessary for write operations
- Automatically handles TOAST decompression and ensures the result is always in regular memory
- Part of PostgreSQL's memory management strategy for preventing unintended modifications to shared data
- Used primarily through the PG_GETARG_MULTIRANGE_P_COPY macro in function implementations