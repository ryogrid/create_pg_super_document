# DatumGetNumericCopy

## Location
[src/include/utils/numeric.h:67-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/numeric.h#L67-L72)

## Overview
DatumGetNumericCopy is an inline function that converts a Datum value to a Numeric pointer, ensuring a writable copy is obtained by handling TOAST decompression and creating a copy when necessary.

## Definition
```c
static inline Numeric
DatumGetNumericCopy(Datum X)
```

## Detailed Description
This function serves as a conversion utility in PostgreSQL's function manager (fmgr) interface to extract a modifiable Numeric data structure from a Datum. Unlike DatumGetNumeric, this function uses PG_DETOAST_DATUM_COPY which ensures that the returned Numeric pointer points to a writable copy of the data. This is crucial when the function needs to modify the numeric value, as the original data might be read-only (especially when stored in TOAST format or when the Datum refers to a constant value). The function handles TOAST decompression automatically and creates a copy in the current memory context when needed.

## Parameters / Member Variables
- `X`: A Datum value containing numeric data that needs to be converted to a writable Numeric pointer

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM_COPY (macro for handling TOAST decompression with copying)
  - Numeric (data type)
- Called from (representative examples):
  - [jsonb_numeric](../j/jsonb_numeric.md)
  - PG_GETARG_NUMERIC_COPY

## Notes and Other Information
- This is an inline function defined in src/include/utils/numeric.h for performance optimization
- Part of the fmgr interface macros used throughout PostgreSQL for type conversions
- Creates a writable copy of the numeric data, making it safe for modifications
- Essential when functions need to modify numeric values rather than just read them
- The copy operation ensures memory safety and prevents modification of shared or read-only data
- More expensive than DatumGetNumeric due to the copying overhead, so should only be used when modification is required