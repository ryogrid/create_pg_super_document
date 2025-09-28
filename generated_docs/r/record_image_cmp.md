# record_image_cmp

## Location
[src/backend/utils/adt/rowtypes.c:1331-1576](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1331-L1576)

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
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md)
  - HeapTupleHeaderGetDatumLength
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - [toast_raw_datum_size](../t/toast_raw_datum_size.md)
  - PG_DETOAST_DATUM_PACKED
  - ReleaseTupleDesc
- Called from (representative examples):
  - [record_image_lt](record_image_lt.md)
  - [record_image_gt](record_image_gt.md)
  - [record_image_le](record_image_le.md)
  - [record_image_ge](record_image_ge.md)
  - [btrecordimagecmp](../b/btrecordimagecmp.md)

## Notes and Other Information
- Returns -1, 0, or 1 for less than, equal, or greater than comparisons respectively
- Used primarily for B-tree indexing where byte-level identity is required
- Handles schema differences like dropped columns gracefully
- Implements memory management for TOAST values to prevent leaks
- The comparison is deterministic and suitable for sorting operations
- Performance optimized with caching of type information between calls
- Located in src/backend/utils/adt/rowtypes.c at lines 1331-1576

## Simplified Source

```c
// Simplified version of record_image_cmp
static int record_image_cmp(FunctionCallInfo fcinfo) {
    HeapTupleHeader record1 = PG_GETARG_HEAPTUPLEHEADER(0);
    HeapTupleHeader record2 = PG_GETARG_HEAPTUPLEHEADER(1);

    // Extract type information from both records
    TypeInfo type1, type2;
    extract_record_type_info(record1, &type1);
    extract_record_type_info(record2, &type2);

    // Setup temporary tuple structures and cached comparison data
    HeapTupleData tuple1, tuple2;
    setup_temp_tuples(&tuple1, &tuple2, record1, record2);
    RecordCompareData *my_extra = setup_comparison_cache(fcinfo, &type1, &type2);

    // Extract column values from both tuples
    Datum *values1, *values2;
    bool *nulls1, *nulls2;
    extract_tuple_values(&tuple1, type1.tupdesc, &values1, &nulls1);
    extract_tuple_values(&tuple2, type2.tupdesc, &values2, &nulls2);

    // Compare columns using byte-level comparison
    int result = 0;
    int i1 = 0, i2 = 0, logical_col = 0;

    while (i1 < type1.ncolumns || i2 < type2.ncolumns) {
        // Skip dropped columns
        skip_dropped_columns(&i1, &i2, type1.tupdesc, type2.tupdesc);
        if (i1 >= type1.ncolumns || i2 >= type2.ncolumns)
            break;

        // Validate column types match
        Form_pg_attribute att1 = TupleDescAttr(type1.tupdesc, i1);
        Form_pg_attribute att2 = TupleDescAttr(type2.tupdesc, i2);
        validate_column_types_match(att1, att2, logical_col);

        // Handle NULL comparison
        if (nulls1[i1] || nulls2[i2]) {
            result = compare_nulls(nulls1[i1], nulls2[i2]);
            if (result != 0) break;
        } else {
            // Byte-level comparison based on storage type
            if (att1->attbyval) {
                // Compare by-value types directly
                result = compare_by_value(values1[i1], values2[i2]);
            } else if (att1->attlen > 0) {
                // Fixed-length types: direct memory comparison
                result = memcmp(DatumGetPointer(values1[i1]),
                               DatumGetPointer(values2[i2]), att1->attlen);
            } else if (att1->attlen == -1) {
                // Variable-length types with TOAST handling
                result = compare_varlena_data(values1[i1], values2[i2]);
            } else {
                elog(ERROR, "unexpected attlen: %d", att1->attlen);
            }

            if (result != 0) break;
        }

        i1++; i2++; logical_col++;
    }

    // Validate final column count consistency
    if (result == 0)
        validate_column_count_match(i1, i2, type1.ncolumns, type2.ncolumns);

    // Cleanup resources
    cleanup_comparison_resources(values1, nulls1, values2, nulls2, &type1, &type2, record1, record2);

    return result;
}
```

Key simplifications made:
- Extracted helper functions for type extraction, tuple setup, and column processing
- Simplified the complex byte comparison logic into separate functions by storage type
- Consolidated TOAST handling for variable-length data into helper function
- Abstracted memory management and caching details
- Focused on the main byte-level comparison algorithm while preserving accuracy