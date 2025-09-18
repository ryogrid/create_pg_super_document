# record_image_cmp

## Location
src/backend/utils/adt/rowtypes.c: 1331 - 1576

## Overview
The `record_image_cmp` function performs internal byte-oriented comparison of PostgreSQL record types, comparing the physical representation of values rather than their logical equality.

## Definition
```c
static int record_image_cmp(FunctionCallInfo fcinfo)
```

## Detailed Description
This function implements a specialized comparison for record/composite types that focuses on the physical byte representation of data rather than logical equality. Unlike regular record comparison, this function considers values with different representations as non-identical, even if they would be logically equal (e.g., 'A' and 'a' in citext type).

The function performs field-by-field comparison using direct memory comparison for efficiency. It handles:
- Type validation ensuring both records have compatible structure
- NULL value comparison (NULLs are considered equal, NULL > non-NULL)  
- Byte-level comparison for by-value types
- Memory comparison for fixed-length types
- Variable-length data comparison with TOAST handling
- Dropped column handling during schema evolution

The function uses caching via `RecordCompareData` to avoid repeated type lookups across multiple calls with the same record types.

## Parameters / Member Variables
- `fcinfo`: FunctionCallInfo containing two HeapTupleHeader arguments representing the records to compare

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetTypeId
  - HeapTupleHeaderGetTypMod
  - lookup_rowtype_tupdesc
  - HeapTupleHeaderGetDatumLength
  - ItemPointerSetInvalid
  - MemoryContextAlloc
  - heap_deform_tuple
  - toast_raw_datum_size
  - PG_DETOAST_DATUM_PACKED
  - ReleaseTupleDesc
- Called from (representative examples):
  - record_image_lt
  - record_image_gt
  - record_image_le
  - record_image_ge
  - btrecordimagecmp

## Notes and Other Information
- Returns -1, 0, or 1 for less than, equal, or greater than comparisons respectively
- Used primarily for B-tree indexing where byte-level identity is required
- Handles schema differences like dropped columns gracefully
- Implements memory management for TOAST values to prevent leaks
- The comparison is deterministic and suitable for sorting operations
- Performance optimized with caching of type information between calls
- Located in src/backend/utils/adt/rowtypes.c at lines 1331-1576