# brin_bloom_consistent

## Location
src/backend/access/brin/brin_bloom.c: 594 - 665

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
  - bloom_get_procinfo
  - bloom_contains_value
  - FunctionCall1Coll
  - DatumGetUInt32
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