# record_image_eq

## Location
[src/backend/utils/adt/rowtypes.c:1577-1752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1577-L1752)

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
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md)
  - HeapTupleHeaderGetDatumLength
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - [datum_image_eq](../d/datum_image_eq.md)
  - ReleaseTupleDesc
  - PG_FREE_IF_COPY
- Called from (representative examples):
  - [record_image_ne](record_image_ne.md)

## Notes and Other Information
- Returns a PostgreSQL Datum boolean value (true for identical, false otherwise)
- Specifically optimized for equality testing, unlike `record_image_cmp` which is for ordering
- Avoids TOAST decompression when possible for better performance
- Handles schema evolution gracefully by skipping dropped columns
- Used primarily for hash indexing and exact match operations
- Performance optimized with early exit on first difference
- Located in src/backend/utils/adt/rowtypes.c at lines 1577-1752

## Simplified Source

```c
Datum record_image_eq(PG_FUNCTION_ARGS) {
    HeapTupleHeader record1 = PG_GETARG_HEAPTUPLEHEADER(0);
    HeapTupleHeader record2 = PG_GETARG_HEAPTUPLEHEADER(1);
    bool result = true;

    // Extract type information from both records
    Oid tupType1 = HeapTupleHeaderGetTypeId(record1);
    Oid tupType2 = HeapTupleHeaderGetTypeId(record2);
    TupleDesc tupdesc1 = lookup_rowtype_tupdesc(tupType1, HeapTupleHeaderGetTypMod(record1));
    TupleDesc tupdesc2 = lookup_rowtype_tupdesc(tupType2, HeapTupleHeaderGetTypMod(record2));

    // Build temporary tuple structures for deformation
    HeapTupleData tuple1 = {HeapTupleHeaderGetDatumLength(record1), InvalidItemPointer, InvalidOid, record1};
    HeapTupleData tuple2 = {HeapTupleHeaderGetDatumLength(record2), InvalidItemPointer, InvalidOid, record2};

    // Decompose tuples into field arrays
    Datum *values1 = palloc(tupdesc1->natts * sizeof(Datum));
    bool *nulls1 = palloc(tupdesc1->natts * sizeof(bool));
    heap_deform_tuple(&tuple1, tupdesc1, values1, nulls1);

    Datum *values2 = palloc(tupdesc2->natts * sizeof(Datum));
    bool *nulls2 = palloc(tupdesc2->natts * sizeof(bool));
    heap_deform_tuple(&tuple2, tupdesc2, values2, nulls2);

    // Compare each field pair, skipping dropped columns
    int i1 = 0, i2 = 0;
    while (i1 < tupdesc1->natts || i2 < tupdesc2->natts) {
        // Skip dropped columns in both tuples
        if (i1 < tupdesc1->natts && TupleDescAttr(tupdesc1, i1)->attisdropped) {
            i1++;
            continue;
        }
        if (i2 < tupdesc2->natts && TupleDescAttr(tupdesc2, i2)->attisdropped) {
            i2++;
            continue;
        }

        // Check for column count mismatch
        if (i1 >= tupdesc1->natts || i2 >= tupdesc2->natts)
            break;

        Form_pg_attribute att1 = TupleDescAttr(tupdesc1, i1);
        Form_pg_attribute att2 = TupleDescAttr(tupdesc2, i2);

        // Verify column types match
        if (att1->atttypid != att2->atttypid)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("cannot compare dissimilar column types")));

        // Handle NULL comparisons (both NULL = equal, one NULL = unequal)
        if (!nulls1[i1] || !nulls2[i2]) {
            if (nulls1[i1] || nulls2[i2]) {
                result = false;
                break;
            }
            // Compare actual data values using byte-level comparison
            result = datum_image_eq(values1[i1], values2[i2], att1->attbyval, att1->attlen);
            if (!result)
                break;
        }

        i1++, i2++;
    }

    // Final validation: ensure equal column counts
    if (result && (i1 != tupdesc1->natts || i2 != tupdesc2->natts))
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("cannot compare record types with different numbers of columns")));

    // Cleanup allocated memory
    pfree(values1);
    pfree(nulls1);
    pfree(values2);
    pfree(nulls2);
    ReleaseTupleDesc(tupdesc1);
    ReleaseTupleDesc(tupdesc2);

    PG_RETURN_BOOL(result);
}
```