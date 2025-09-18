# test_destroy

## Location
[src/test/modules/test_tidstore/test_tidstore.c:327-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_tidstore/test_tidstore.c#L327-L339)

## Overview
A cleanup function that properly destroys the TidStore and frees all associated memory resources, including verification arrays used in testing.

## Definition


## Detailed Description
This function performs comprehensive cleanup of the TidStore testing environment by destroying the main TidStore instance and releasing all allocated memory for verification arrays. It ensures proper resource management by freeing both the TidStore itself and the auxiliary data structures used for testing validation.

The function follows PostgreSQL's memory management practices by using pfree() to release dynamically allocated memory and sets the tidstore pointer to NULL to prevent accidental reuse after destruction.

## Parameters / Member Variables
- No explicit parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)
- Operates on global  and  structures
- Resets global state including tidstore pointer and item counts

## Dependencies
- Functions called/Symbols referenced:
  - [check_tidstore_available](../c/check_tidstore_available.md) - Validates that tidstore exists before destruction
  - [TidStoreDestroy](../T/TidStoreDestroy.md) - Core function to destroy the TidStore instance
  - [pfree](../p/pfree.md) - PostgreSQL memory deallocation function (used 3 times for verification arrays)
- Called from (representative examples):
  - No direct references found (likely called via SQL interface in tests)

## Notes and Other Information
- Located in src/test/modules/test_tidstore/test_tidstore.c:327-339
- Essential for proper resource cleanup in the TidStore testing framework
- Frees three verification arrays: insert_tids, lookup_tids, and iter_tids
- Resets the item count to zero and nullifies the tidstore pointer to prevent use-after-free
- Part of the test lifecycle management, typically called at the end of test scenarios
- Ensures no memory leaks in the testing infrastructure
- Returns void as its primary purpose is cleanup rather than data processing
- Critical for maintaining clean test state between different test runs