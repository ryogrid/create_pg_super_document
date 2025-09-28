# record_cmp

## Location
[src/backend/utils/adt/rowtypes.c:823-1066](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L823-L1066)

## Overview
Internal comparison function for composite type (record) values that implements element-by-element comparison logic for PostgreSQL's record comparison operations.

## Definition

```c
structures */
	tuple1.t_len = HeapTupleHeaderGetDatumLength(record1);
```
## Detailed Description
The  function is the core comparison engine for composite types in PostgreSQL. It performs a lexicographic comparison between two record values by comparing corresponding columns in order. The function handles different record types as long as they have the same number of non-dropped columns with compatible types. It implements PostgreSQL's NULL comparison semantics where NULL values are considered greater than any non-NULL value, and two NULL values are considered equal.

The function extracts type information from both tuple headers, validates column type compatibility, and performs element-by-element comparison using the appropriate type-specific comparison functions. It handles dropped columns by skipping them and maintains comparison metadata cache for performance optimization across repeated calls.

## Parameters / Member Variables
- : Function call information containing two  arguments representing the records to compare
- Returns: Integer (-1, 0, 1) indicating first record is less than, equal to, or greater than second record

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md): Stack overflow protection for recursive calls
  - HeapTupleHeaderGetTypeId: Extracts type OID from tuple headers
  - HeapTupleHeaderGetTypMod: Extracts type modifier from tuple headers  
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md): Retrieves tuple descriptors for both record types
  - [heap_deform_tuple](../h/heap_deform_tuple.md): Extracts individual column values from both tuples
  - [lookup_type_cache](../l/lookup_type_cache.md): Gets type cache entries with comparison function info
  - FunctionCallInvoke: Invokes type-specific comparison functions
  - [format_type_be](../f/format_type_be.md): Formats type names for error messages
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Memory allocation in function context
  - ReleaseTupleDesc: Releases tuple descriptor references

- Called from (representative examples):
  - [record_lt](record_lt.md): Less than comparison operator for records (src/backend/utils/adt/rowtypes.c:1291)
  - [record_gt](record_gt.md): Greater than comparison operator for records (src/backend/utils/adt/rowtypes.c:1297)
  - [record_le](record_le.md): Less than or equal comparison operator for records (src/backend/utils/adt/rowtypes.c:1303)
  - [record_ge](record_ge.md): Greater than or equal comparison operator for records (src/backend/utils/adt/rowtypes.c:1309)
  - [btrecordcmp](../b/btrecordcmp.md): B-tree comparison function for records (src/backend/utils/adt/rowtypes.c:1315)

## Notes and Other Information
- Implements lexicographic comparison: compares columns left-to-right until finding unequal values
- Handles heterogeneous record types (e.g., anonymous ROW() vs named composite types) 
- Enforces strict type compatibility: corresponding columns must have identical type OIDs
- Uses PostgreSQL's NULL comparison semantics: NULL > any non-NULL value, NULL == NULL
- Handles collation differences gracefully by passing InvalidOid when collations don't match
- Uses function-local caching (fn_extra) to optimize repeated calls with same record types
- Validates column count consistency between record types
- Memory management includes protection against toasted input values
- Core foundation for all record comparison operators and B-tree indexing support

## Simplified Source

```c
// Simplified version of record_cmp
static int record_cmp(FunctionCallInfo fcinfo) {
    HeapTupleHeader record1 = PG_GETARG_HEAPTUPLEHEADER(0);
    HeapTupleHeader record2 = PG_GETARG_HEAPTUPLEHEADER(1);

    // Extract type information from both records
    TypeInfo type1, type2;
    extract_record_type_info(record1, &type1);
    extract_record_type_info(record2, &type2);

    // Setup temporary tuple structures
    HeapTupleData tuple1, tuple2;
    setup_temp_tuples(&tuple1, &tuple2, record1, record2);

    // Setup or reuse cached comparison information
    RecordCompareData *my_extra = setup_comparison_cache(fcinfo, &type1, &type2);

    // Extract column values from both tuples
    Datum *values1, *values2;
    bool *nulls1, *nulls2;
    extract_tuple_values(&tuple1, type1.tupdesc, &values1, &nulls1);
    extract_tuple_values(&tuple2, type2.tupdesc, &values2, &nulls2);

    // Compare columns element by element
    int result = 0;
    int i1 = 0, i2 = 0, logical_col = 0;

    while (i1 < type1.ncolumns || i2 < type2.ncolumns) {
        // Skip dropped columns
        skip_dropped_columns(&i1, &i2, type1.tupdesc, type2.tupdesc);

        // Check for column count mismatch
        if (i1 >= type1.ncolumns || i2 >= type2.ncolumns)
            break;

        // Get column attributes and validate types match
        Form_pg_attribute att1 = TupleDescAttr(type1.tupdesc, i1);
        Form_pg_attribute att2 = TupleDescAttr(type2.tupdesc, i2);
        validate_column_types_match(att1, att2, logical_col);

        // Get comparison function for this column type
        TypeCacheEntry *typentry = get_column_comparison_function(my_extra, logical_col, att1->atttypid);

        // Handle NULL comparison: NULL > non-NULL, NULL == NULL
        if (nulls1[i1] || nulls2[i2]) {
            result = compare_nulls(nulls1[i1], nulls2[i2]);
            if (result != 0) break;
        } else {
            // Compare non-NULL values using type-specific function
            Oid collation = resolve_collation(att1, att2);
            result = call_comparison_function(typentry, values1[i1], values2[i2], collation);
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
- Extracted helper functions for type extraction, tuple setup, and value extraction
- Simplified NULL comparison logic into dedicated function
- Consolidated column validation and comparison function lookup
- Abstracted complex memory management and caching details
- Focused on the main comparison algorithm flow while preserving correctness