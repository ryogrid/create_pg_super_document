# brin_bloom_consistent

## Location
[src/backend/access/brin/brin_bloom.c:594-665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_bloom.c#L594-L665)

## Overview
Determines whether scan keys are consistent with an index tuple's bloom filter, used during BRIN index scans to eliminate page ranges that cannot contain matching values.

## Definition
```c
Datum brin_bloom_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a BRIN operator function that implements the consistency check for bloom filter-based BRIN indexes. It evaluates whether a given page range (represented by its bloom filter summary) might contain tuples matching the scan conditions:

1. **Filter extraction**: Retrieves the bloom filter from the BrinValues structure, handling TOAST decompression if necessary
2. **Multi-key evaluation**: Iterates through all scan keys, applying the appropriate strategy for each
3. **Hash-based lookup**: For equality strategies, computes the hash of the search value and checks if it might be contained in the bloom filter
4. **Early termination**: Stops processing as soon as any key fails the consistency check (short-circuit evaluation)

The function assumes all keys match initially and looks for evidence that the page range can be eliminated from the scan.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL function argument structure containing:
  - `bdesc`: BRIN descriptor with index metadata
  - `column`: BrinValues structure containing the bloom filter for this range
  - `keys`: Array of ScanKey structures representing the search conditions
  - `nkeys`: Number of scan keys to process

## Dependencies
- Functions called/Symbols referenced:
  - [bloom_get_procinfo](bloom_get_procinfo.md)
  - [bloom_contains_value](bloom_contains_value.md)
  - [FunctionCall1Coll](../F/FunctionCall1Coll.md)
  - [DatumGetUInt32](../D/DatumGetUInt32.md)
  - PG_DETOAST_DATUM
  - BloomEqualStrategyNumber
  - PROCNUM_HASH
- Called from (representative examples):
  - This is a BRIN operator function called by the BRIN scanning infrastructure

## Notes and Other Information
- This is a PostgreSQL internal function following the PG_FUNCTION_ARGS convention for operator functions
- Currently only supports BloomEqualStrategyNumber strategy (equality comparisons)
- Uses assertions to ensure NULL keys are not processed, as they should be filtered out by bringetbitmap
- The function implements a conservative approach: it may return false positives (indicating a match when none exists) but never false negatives
- Early termination optimization: stops checking additional keys once any key fails the consistency check
- Error handling for unsupported strategies with elog(ERROR, ...)
- The bloom filter's probabilistic nature means this function may indicate matches for ranges that don't actually contain the sought value, but it will never miss ranges that do contain the value

## Simplified Source

```c
Datum brin_bloom_consistent(PG_FUNCTION_ARGS) {
    BrinDesc *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
    BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
    ScanKey *keys = (ScanKey *) PG_GETARG_POINTER(2);
    int nkeys = PG_GETARG_INT32(3);
    Oid colloid = PG_GET_COLLATION();

    // Extract bloom filter from column data
    BloomFilter *filter = (BloomFilter *) PG_DETOAST_DATUM(column->bv_values[0]);

    // Assume all keys match, look for eliminating evidence
    bool matches = true;

    // Check each scan key
    for (int keyno = 0; keyno < nkeys; keyno++) {
        ScanKey key = keys[keyno];
        AttrNumber attno = key->sk_attno;
        Datum value = key->sk_argument;

        switch (key->sk_strategy) {
            case BloomEqualStrategyNumber:
                // Hash the search value and check if bloom filter contains it
                FmgrInfo *finfo = bloom_get_procinfo(bdesc, attno, PROCNUM_HASH);
                uint32 hashValue = DatumGetUInt32(FunctionCall1Coll(finfo, colloid, value));
                matches &= bloom_contains_value(filter, hashValue);
                break;

            default:
                elog(ERROR, "invalid strategy number %d", key->sk_strategy);
                matches = false;
                break;
        }

        // Early exit if any key fails
        if (!matches)
            break;
    }

    PG_RETURN_BOOL(matches);
}
```