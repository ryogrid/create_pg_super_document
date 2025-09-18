# record_image_eq

## Location
src/backend/utils/adt/rowtypes.c: 1577 - 1752

## Overview
The `record_image_eq` function compares two PostgreSQL records for identical byte-level content, returning true only when both records have exactly the same physical representation.

## Definition
```c
Datum record_image_eq(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs byte-oriented equality comparison for PostgreSQL record/composite types. Unlike logical equality comparison, this function requires identical physical representation of data values. The function is optimized for equality testing and avoids unnecessary TOAST de-compression when records have different lengths.

The comparison process includes:
- Type validation to ensure both records have compatible structure
- Field-by-field comparison using `datum_image_eq` for actual data values
- NULL handling (both NULL values are considered equal, one NULL makes records unequal)
- Dropped column handling during schema evolution
- Early termination on first inequality for performance

The function uses caching via `RecordCompareData` to avoid repeated type lookups across multiple calls with the same record types.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing two HeapTupleHeader records to compare

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetTypeId
  - HeapTupleHeaderGetTypMod
  - lookup_rowtype_tupdesc
  - HeapTupleHeaderGetDatumLength
  - ItemPointerSetInvalid
  - MemoryContextAlloc
  - heap_deform_tuple
  - datum_image_eq
  - ReleaseTupleDesc
  - PG_FREE_IF_COPY
- Called from (representative examples):
  - record_image_ne

## Notes and Other Information
- Returns a PostgreSQL Datum boolean value (true for identical, false otherwise)
- Specifically optimized for equality testing, unlike `record_image_cmp` which is for ordering
- Avoids TOAST decompression when possible for better performance
- Handles schema evolution gracefully by skipping dropped columns
- Used primarily for hash indexing and exact match operations
- Performance optimized with early exit on first difference
- Located in src/backend/utils/adt/rowtypes.c at lines 1577-1752