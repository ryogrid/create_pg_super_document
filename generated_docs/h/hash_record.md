# hash_record

## Location
[src/backend/utils/adt/rowtypes.c:1794-1913](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1794-L1913)

## Overview
The hash_record function computes a hash value for a composite type (record) by combining hash values of all its non-dropped columns.

## Definition

```c
structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(record);
```
## Detailed Description
This function implements hash computation for PostgreSQL record (composite) types. It extracts the tuple structure from the input record, decomposes it into individual column values, and computes a combined hash by calling the appropriate hash function for each column's data type. The function uses caching mechanisms to avoid repeated lookups of type information and hash function details across multiple calls on the same record type.

The hash computation follows the same algorithm as hash_array(), using a left-shift and subtraction pattern:  for each column. NULL values contribute a hash of 0 to the final result.

## Parameters / Member Variables
- : Standard PostgreSQL function call information containing the record to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetTypeId: Extract type OID from tuple header
  - HeapTupleHeaderGetTypMod: Extract type modifier from tuple header  
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md): Get tuple descriptor for the record type
  - [heap_deform_tuple](heap_deform_tuple.md): Break down tuple into individual column values
  - [lookup_type_cache](../l/lookup_type_cache.md): Get type cache entry with hash function info
  - FunctionCallInvoke: Call the hash function for each column
  - [check_stack_depth](../c/check_stack_depth.md): Prevent stack overflow in recursive calls
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Uses RecordCompareData structure for caching type information and hash function details between calls
- Handles dropped columns by skipping them during hash computation
- Includes stack depth checking to prevent infinite recursion when hashing nested record types
- Memory management includes cleanup of temporary allocations and handling of toasted input
- Returns a 32-bit unsigned integer hash value
- Located in src/backend/utils/adt/rowtypes.c:1794-1913

## Simplified Source

```c
// Simplified version of hash_record
Datum hash_record(PG_FUNCTION_ARGS) {
    HeapTupleHeader record = PG_GETARG_HEAPTUPLEHEADER(0);
    uint32 result = 0;

    check_stack_depth();  // Prevent infinite recursion

    // Extract type information and setup tuple
    Oid tupType = HeapTupleHeaderGetTypeId(record);
    int32 tupTypmod = HeapTupleHeaderGetTypMod(record);
    TupleDesc tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    int ncolumns = tupdesc->natts;

    HeapTupleData tuple;
    setup_temp_tuple(&tuple, record);

    // Setup or reuse cached hash function information
    RecordCompareData *my_extra = setup_hash_cache(fcinfo, ncolumns, tupType, tupTypmod);

    // Extract column values
    Datum *values = (Datum *) palloc(ncolumns * sizeof(Datum));
    bool *nulls = (bool *) palloc(ncolumns * sizeof(bool));
    heap_deform_tuple(&tuple, tupdesc, values, nulls);

    // Compute hash for each non-dropped column
    for (int i = 0; i < ncolumns; i++) {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);

        // Skip dropped columns
        if (att->attisdropped)
            continue;

        // Get hash function for this column type
        TypeCacheEntry *typentry = get_column_hash_function(my_extra, i, att->atttypid);

        // Compute column hash value
        uint32 element_hash;
        if (nulls[i]) {
            element_hash = 0;  // NULL values contribute 0
        } else {
            element_hash = call_hash_function(typentry, values[i], att->attcollation);
        }

        // Combine with accumulated hash using array-style algorithm
        result = (result << 5) - result + element_hash;
    }

    // Cleanup
    cleanup_hash_resources(values, nulls, tupdesc, record);

    PG_RETURN_UINT32(result);
}
```

Key simplifications made:
- Extracted helper functions for tuple setup, cache management, and hash function lookup
- Simplified the column iteration and hash combination logic
- Consolidated type cache handling and hash function calls
- Abstracted memory management details
- Focused on the core hash computation algorithm while preserving correctness