# record_eq

## Location
[src/backend/utils/adt/rowtypes.c:1067-1282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1067-L1282)

## Overview
Compares two records (row types) for equality, returning true if all corresponding column values are equal.

## Definition

```c
structures */
	tuple1.t_len = HeapTupleHeaderGetDatumLength(record1);
```
## Detailed Description
The  function performs field-by-field comparison of two PostgreSQL records to determine equality. It handles records with potentially different structures by:

1. Extracting type information from both record headers
2. Building temporary HeapTuple control structures
3. Caching comparison metadata to optimize repeated calls
4. Deforming tuples into individual column values and null flags
5. Comparing corresponding columns while handling:
   - Dropped columns (skipped during comparison)
   - Type mismatches (raises error)
   - NULL values (two NULLs are considered equal, NULL ≠ non-NULL)
   - Column count mismatches (raises error if structures differ)

The function uses the type cache system to look up appropriate equality operators for each column type and employs stack depth checking to prevent infinite recursion when dealing with nested record types.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention containing:
  - : First HeapTupleHeader to compare (argument 0)
  - : Second HeapTupleHeader to compare (argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - : Prevents stack overflow in recursive comparisons
  - : Extracts record type OID
  - : Extracts type modifier
  - : Gets tuple descriptor for record type
  - : Breaks record into individual column values
  - : Caches equality operator information
  - : Allocates comparison metadata cache
  - : Calls column-specific equality functions
- Called from (representative examples):
  - : Uses record_eq and negates the result

## Notes and Other Information
- Does not use  for comparison since equality can be meaningful for types without total ordering
- Caches comparison metadata () in  to optimize repeated calls with same record types
- Handles structural differences gracefully by skipping dropped columns
- Raises errors for type mismatches and column count differences
- Memory management includes cleanup of temporary allocations and toasted input handling
- Supports collation-aware comparisons when column collations match

## Simplified Source

```c
// Simplified version of record_eq
Datum record_eq(PG_FUNCTION_ARGS) {
    HeapTupleHeader record1 = PG_GETARG_HEAPTUPLEHEADER(0);
    HeapTupleHeader record2 = PG_GETARG_HEAPTUPLEHEADER(1);
    bool result = true;

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

    // Compare columns for equality
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

        // Get equality function for this column type
        TypeCacheEntry *typentry = get_column_equality_function(my_extra, logical_col, att1->atttypid);

        // Handle NULL values: both NULL = equal, one NULL = not equal
        if (nulls1[i1] || nulls2[i2]) {
            if (nulls1[i1] != nulls2[i2]) {
                result = false;
                break;
            }
            // Both NULL - continue to next column
        } else {
            // Compare non-NULL values using type-specific equality function
            Oid collation = resolve_collation(att1, att2);
            bool equal = call_equality_function(typentry, values1[i1], values2[i2], collation);
            if (!equal) {
                result = false;
                break;
            }
        }

        i1++; i2++; logical_col++;
    }

    // Validate final column count consistency if still equal
    if (result)
        validate_column_count_match(i1, i2, type1.ncolumns, type2.ncolumns);

    // Cleanup resources
    cleanup_comparison_resources(values1, nulls1, values2, nulls2, &type1, &type2, record1, record2);

    PG_RETURN_BOOL(result);
}
```

Key simplifications made:
- Reused helper functions from record_cmp for common operations
- Simplified equality-specific logic (no ordering, just equal/not-equal)
- Consolidated NULL handling for equality semantics
- Extracted equality function lookup separate from comparison functions
- Focused on the equality check flow while maintaining type safety