# DatumGetAnyArrayP

## Location
[src/backend/utils/adt/array_expanded.c:401-423](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_expanded.c#L401-L423)

## Overview
DatumGetAnyArrayP converts a PostgreSQL Datum to an AnyArrayType pointer, returning either an expanded array header or a detoasted varlena array depending on the input format, with the result intended for read-only access.

## Definition
```c
AnyArrayType *DatumGetAnyArrayP(Datum d)
```

## Detailed Description
This function provides a unified interface for accessing PostgreSQL arrays regardless of their internal storage format. It first checks if the input Datum represents an expanded array (either read-write or read-only). If so, it extracts and returns the ExpandedArrayHeader directly cast to AnyArrayType. If the Datum contains a regular varlena array, it performs detoasting if necessary using PG_DETOAST_DATUM and returns the result as AnyArrayType.

The function is designed for scenarios where code needs to work with arrays without modifying them in-place, providing a consistent interface that abstracts away the underlying storage format differences between regular and expanded arrays.

## Parameters / Member Variables
- `d`: The input Datum containing an array in any supported format (regular varlena or expanded)

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTERNAL_EXPANDED (checks if Datum is expanded array of any type)
  - [DatumGetEOHP](DatumGetEOHP.md) (extracts expanded object header pointer)
  - PG_DETOAST_DATUM (detoasts regular varlena arrays)
  - [DatumGetPointer](DatumGetPointer.md) (extracts pointer from Datum)
- Called from (representative examples):
  - [array_map](../a/array_map.md) (array mapping operations)
  - PG_GETARG_ANY_ARRAY_P (macro for function argument processing)
  - AARR_LBOUND (array lower bound access macro)

## Notes and Other Information
- The returned AnyArrayType is a union that can represent either ArrayType (regular) or ExpandedArrayHeader (expanded)
- The function explicitly states that results must not be modified in-place, making it suitable for read-only operations
- EA_MAGIC assertion validates expanded array headers for corruption detection
- The function handles both read-write and read-only expanded arrays uniformly
- Detoasting is performed automatically for compressed or externally stored regular arrays
- This abstraction allows callers to work with arrays without knowing their specific storage format