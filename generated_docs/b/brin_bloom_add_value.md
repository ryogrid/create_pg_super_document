# brin_bloom_add_value

## Location
[src/backend/access/brin/brin_bloom.c:539-593](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_bloom.c#L539-L593)

## Overview
Examines an index tuple and updates its bloom filter by adding a new value from a heap tuple, returning whether the bloom filter was modified.

## Definition
```c
Datum brin_bloom_add_value(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a BRIN operator function that maintains bloom filter summaries for index ranges. It processes a new value from a heap tuple and determines if it needs to update the existing bloom filter:

1. **Initialization**: If this is the first non-null value for the range, initializes a new bloom filter using calculated ndistinct and false positive rate parameters
2. **Hash computation**: Computes a hash value for the new input using the appropriate hash function for the column's data type
3. **Filter update**: Adds the hash value to the bloom filter and determines if the filter was actually modified
4. **State management**: Updates the BrinValues structure with the new or modified bloom filter

The function handles the transition from all-null state to having actual data and ensures the bloom filter is properly maintained throughout the life of the BRIN index.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL function argument structure containing:
  - `bdesc`: BRIN descriptor with index metadata
  - `column`: BrinValues structure containing current bloom filter state
  - `newval`: New datum value to potentially add to the bloom filter
  - `isnull`: Boolean indicating if the new value is null (used for assertions)

## Dependencies
- Functions called/Symbols referenced:
  - [brin_bloom_get_ndistinct](brin_bloom_get_ndistinct.md)
  - BloomGetFalsePositiveRate
  - [bloom_init](bloom_init.md)
  - [bloom_get_procinfo](bloom_get_procinfo.md)
  - [bloom_add_value](bloom_add_value.md)
  - [FunctionCall1Coll](../F/FunctionCall1Coll.md)
  - [DatumGetUInt32](../D/DatumGetUInt32.md)
  - PG_DETOAST_DATUM
- Called from (representative examples):
  - This is a BRIN operator function called by the BRIN indexing infrastructure

## Notes and Other Information
- This is a PostgreSQL internal function that follows the PG_FUNCTION_ARGS convention for operator functions
- The function is designed to be called during BRIN index construction and maintenance
- Uses assertions to verify that null values are not processed (Assert(!isnull))
- Handles TOAST decompression for existing bloom filters that may be stored compressed
- The return value indicates whether the bloom filter summary was actually updated, which is important for BRIN's change tracking
- The function manages memory by ensuring the updated bloom filter is properly stored back in the BrinValues structure

## Simplified Source

```c
Datum brin_bloom_add_value(PG_FUNCTION_ARGS) {
    BrinDesc *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
    BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
    Datum newval = PG_GETARG_DATUM(2);
    BloomOptions *opts = (BloomOptions *) PG_GET_OPCLASS_OPTIONS();
    Oid colloid = PG_GET_COLLATION();

    bool updated = false;
    AttrNumber attno = column->bv_attno;
    BloomFilter *filter;

    // Initialize bloom filter if this is first non-null value
    if (column->bv_allnulls) {
        filter = bloom_init(brin_bloom_get_ndistinct(bdesc, opts),
                           BloomGetFalsePositiveRate(opts));
        column->bv_values[0] = PointerGetDatum(filter);
        column->bv_allnulls = false;
        updated = true;
    } else {
        // Extract existing bloom filter
        filter = (BloomFilter *) PG_DETOAST_DATUM(column->bv_values[0]);
    }

    // Compute hash of new value and add to bloom filter
    FmgrInfo *hashFn = bloom_get_procinfo(bdesc, attno, PROCNUM_HASH);
    uint32 hashValue = DatumGetUInt32(FunctionCall1Coll(hashFn, colloid, newval));
    filter = bloom_add_value(filter, hashValue, &updated);

    // Store updated filter back
    column->bv_values[0] = PointerGetDatum(filter);

    PG_RETURN_BOOL(updated);
}
```