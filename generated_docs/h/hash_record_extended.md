# hash_record_extended

## Location
[src/backend/utils/adt/rowtypes.c:1914-2034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1914-L2034)

## Overview
The hash_record_extended function computes a seeded hash value for a composite type (record) using extended hash functions that accept a seed parameter for enhanced hash distribution.

## Definition

```c
structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(record);
```
## Detailed Description
This function is the extended version of hash_record that supports seeded hashing. It takes both a record and a 64-bit seed value as input parameters. The function decomposes the record into individual columns and computes a combined hash by calling the extended hash function for each column's data type, passing the seed value to ensure better hash distribution and collision resistance.

Like its non-extended counterpart, it uses caching mechanisms to avoid repeated lookups of type information and hash function details. The hash computation follows the same left-shift algorithm: . The key difference is the use of TYPECACHE_HASH_EXTENDED_PROC_FINFO to access extended hash functions and the return of a 64-bit hash value instead of 32-bit.

## Parameters / Member Variables
- : Standard PostgreSQL function call information containing:
  - Argument 0: The record (HeapTupleHeader) to be hashed
  - Argument 1: The 64-bit seed value (int64) for hash computation

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetTypeId: Extract type OID from tuple header
  - HeapTupleHeaderGetTypMod: Extract type modifier from tuple header
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md): Get tuple descriptor for the record type
  - [heap_deform_tuple](heap_deform_tuple.md): Break down tuple into individual column values
  - [lookup_type_cache](../l/lookup_type_cache.md): Get type cache entry with extended hash function info
  - FunctionCallInvoke: Call the extended hash function for each column
  - [check_stack_depth](../c/check_stack_depth.md): Prevent stack overflow in recursive calls
  - [Int64GetDatum](../I/Int64GetDatum.md): Convert seed value to Datum for function calls
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Extended version of hash_record that supports seeded hashing for improved hash distribution
- Uses TYPECACHE_HASH_EXTENDED_PROC_FINFO instead of TYPECACHE_HASH_PROC_FINFO 
- Returns a 64-bit unsigned integer hash value instead of 32-bit
- Passes seed parameter to each column's extended hash function
- Includes the same caching, memory management, and error handling as hash_record
- Handles dropped columns by skipping them during hash computation
- Located in src/backend/utils/adt/rowtypes.c:1914-2034

## Simplified Source

```c
Datum hash_record_extended(PG_FUNCTION_ARGS) {
    HeapTupleHeader record = PG_GETARG_HEAPTUPLEHEADER(0);
    uint64 seed = PG_GETARG_INT64(1);
    uint64 result = 0;

    check_stack_depth(); // Prevent recursion overflow

    // Extract type information from record
    Oid tupType = HeapTupleHeaderGetTypeId(record);
    int32 tupTypmod = HeapTupleHeaderGetTypMod(record);
    TupleDesc tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    int ncolumns = tupdesc->natts;

    // Build temporary tuple structure
    HeapTupleData tuple = {
        HeapTupleHeaderGetDatumLength(record),
        InvalidItemPointer,
        InvalidOid,
        record
    };

    // Cache type information for performance
    RecordCompareData *my_extra = (RecordCompareData *) fcinfo->flinfo->fn_extra;
    if (my_extra == NULL || my_extra->ncolumns < ncolumns) {
        my_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
            offsetof(RecordCompareData, columns) + ncolumns * sizeof(ColumnCompareData));
        fcinfo->flinfo->fn_extra = my_extra;
        my_extra->ncolumns = ncolumns;
        my_extra->record1_type = InvalidOid;
    }

    // Reset cache if record type changed
    if (my_extra->record1_type != tupType || my_extra->record1_typmod != tupTypmod) {
        MemSet(my_extra->columns, 0, ncolumns * sizeof(ColumnCompareData));
        my_extra->record1_type = tupType;
        my_extra->record1_typmod = tupTypmod;
    }

    // Decompose tuple into field arrays
    Datum *values = palloc(ncolumns * sizeof(Datum));
    bool *nulls = palloc(ncolumns * sizeof(bool));
    heap_deform_tuple(&tuple, tupdesc, values, nulls);

    // Hash each column and combine results
    for (int i = 0; i < ncolumns; i++) {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        uint64 element_hash;

        // Skip dropped columns
        if (att->attisdropped)
            continue;

        // Lookup or retrieve cached hash function
        TypeCacheEntry *typentry = my_extra->columns[i].typentry;
        if (typentry == NULL || typentry->type_id != att->atttypid) {
            typentry = lookup_type_cache(att->atttypid, TYPECACHE_HASH_EXTENDED_PROC_FINFO);
            if (!OidIsValid(typentry->hash_extended_proc_finfo.fn_oid))
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                    errmsg("could not identify an extended hash function for type")));
            my_extra->columns[i].typentry = typentry;
        }

        // Compute hash for this column
        if (nulls[i]) {
            element_hash = 0; // NULL values hash to 0
        } else {
            // Call extended hash function with seed
            LOCAL_FCINFO(locfcinfo, 2);
            InitFunctionCallInfoData(*locfcinfo, &typentry->hash_extended_proc_finfo,
                2, att->attcollation, NULL, NULL);
            locfcinfo->args[0].value = values[i];
            locfcinfo->args[0].isnull = false;
            locfcinfo->args[1].value = Int64GetDatum(seed);
            locfcinfo->args[1].isnull = false;
            element_hash = DatumGetUInt64(FunctionCallInvoke(locfcinfo));
        }

        // Combine hash using left-shift algorithm
        result = (result << 5) - result + element_hash;
    }

    // Cleanup memory
    pfree(values);
    pfree(nulls);
    ReleaseTupleDesc(tupdesc);
    PG_FREE_IF_COPY(record, 0);

    PG_RETURN_UINT64(result);
}
```