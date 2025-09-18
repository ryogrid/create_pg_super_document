# brin_bloom_add_value

## Location
src/backend/access/brin/brin_bloom.c: 539 - 593

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
  - brin_bloom_get_ndistinct
  - BloomGetFalsePositiveRate
  - bloom_init
  - bloom_get_procinfo
  - bloom_add_value
  - FunctionCall1Coll
  - DatumGetUInt32
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