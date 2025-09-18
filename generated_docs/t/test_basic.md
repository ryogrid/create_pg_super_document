# test_basic

## Location
src/test/modules/test_radixtree/test_radixtree.c: 170 - 298

## Overview
A comprehensive test function that validates basic radix tree operations including insertion, lookup, update, deletion, and iteration with configurable key patterns and node configurations.

## Definition
```c
static void test_basic(rt_node_class_test_elem *test_info, int shift, bool asc)
```

## Detailed Description
This function performs extensive testing of radix tree functionality with a specific node configuration. It creates a radix tree and tests the complete lifecycle of key-value operations:
1. Inserts keys with specified shift and ordering (ascending/descending)
2. Verifies lookups return correct values
3. Updates existing keys and validates changes
4. Deletes and re-inserts keys to test deletion/insertion consistency
5. Tests iterator functionality with proper key ordering
6. Performs final cleanup by deleting all keys and verifying empty state
The function supports both local and shared radix tree configurations and provides detailed logging of test parameters.

## Parameters / Member Variables
- `test_info`: Pointer to rt_node_class_test_elem structure containing test configuration (node class name, number of keys)
- `shift`: Bit shift value applied to key generation for testing different key distributions
- `asc`: Boolean flag determining key insertion order (true for ascending, false for descending)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (memory context management)
  - rt_create (radix tree creation)
  - rt_set (key insertion and updates)
  - rt_find (key lookup operations)
  - rt_delete (key deletion operations)
  - rt_begin_iterate, rt_iterate_next, rt_end_iterate (iteration operations)
  - rt_stats (statistics reporting)
  - rt_free (radix tree cleanup)
  - [palloc](../p/palloc.md), pfree (memory allocation/deallocation)
  - elog (logging functionality)
  - LWLockNewTrancheId, LWLockRegisterTranche, dsa_create, dsa_detach (shared memory operations)
- Called from (representative examples):
  - test_radixtree (main test function, called multiple times with different parameters)

## Notes and Other Information
- This is a static function used internally within the test_radixtree module
- Uses EXPECT_TRUE, EXPECT_FALSE, and EXPECT_EQ_U64 macros for test assertions
- Supports both local and shared radix tree testing via TEST_SHARED_RT compilation flag
- Tests are parameterized to cover different node types, key shifts, and insertion patterns
- Validates that iteration returns keys in sorted order regardless of insertion order
- Performs comprehensive validation of radix tree consistency across all operations
- Uses TestValueType for value storage, with values typically equal to their corresponding keys