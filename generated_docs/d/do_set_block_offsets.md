# do_set_block_offsets

## Location
[src/test/modules/test_tidstore/test_tidstore.c:170-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_tidstore/test_tidstore.c#L170-L219)

## Overview
A PostgreSQL test function that sets TID (tuple identifier) entries for a given block number and array of offsets in the TidStore testing framework.

## Definition


## Detailed Description
This function is part of the test_tidstore module, designed to test TidStore functionality. It takes a block number and an array of offsets, then stores the corresponding TIDs both in the actual TidStore and in a verification array for testing purposes. The function performs exclusive locking during the TidStore operation to ensure thread safety and maintains parallel data structures for verification.

The function first validates the input parameters, extracts offset data from the PostgreSQL array type, then performs the actual TID storage operations. It also manages memory allocation for verification arrays, dynamically expanding them as needed.

## Parameters / Member Variables
- : Block number (BlockNumber) where the TIDs will be stored
- : PostgreSQL array containing offset numbers within the block

## Dependencies
- Functions called/Symbols referenced:
  - [check_tidstore_available](../c/check_tidstore_available.md) - Validates that tidstore is available for operations  
  - [sanity_check_array](../s/sanity_check_array.md) - Validates the input array structure
  - [TidStoreLockExclusive](../T/TidStoreLockExclusive.md) - Acquires exclusive lock on the TidStore
  - [TidStoreSetBlockOffsets](../T/TidStoreSetBlockOffsets.md) - Core function to set block offsets in TidStore
  - [TidStoreUnlock](../T/TidStoreUnlock.md) - Releases the TidStore lock
  - [purge_from_verification_array](../p/purge_from_verification_array.md) - Removes existing entries from verification data
  - ArrayGetNItems, ARR_NDIM, ARR_DIMS, ARR_DATA_PTR - PostgreSQL array manipulation functions
  - [ItemPointerSet](../I/ItemPointerSet.md) - Sets tuple identifier values
  - [repalloc](../r/repalloc.md) - PostgreSQL memory reallocation function
- Called from (representative examples):
  - No direct references found (likely called via SQL interface in tests)

## Notes and Other Information
- This is a test-specific function located in src/test/modules/test_tidstore/test_tidstore.c:170-219
- Uses exclusive locking to ensure thread-safe operations on the TidStore
- Maintains parallel verification arrays (insert_tids, lookup_tids, iter_tids) for testing validation
- Dynamically expands verification arrays using a doubling strategy when capacity is exceeded
- Returns the block number as a PostgreSQL Datum for SQL interface compatibility
- Part of the PostgreSQL testing infrastructure specifically for TidStore functionality validation