# test_basic

## Location
[src/test/modules/test_radixtree/test_radixtree.c:170-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_radixtree/test_radixtree.c#L170-L298)

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
  - [LWLockNewTrancheId](../L/LWLockNewTrancheId.md), LWLockRegisterTranche, dsa_create, dsa_detach (shared memory operations)
- Called from (representative examples):
  - [test_radixtree](test_radixtree.md) (main test function, called multiple times with different parameters)

## Notes and Other Information
- This is a static function used internally within the test_radixtree module
- Uses EXPECT_TRUE, EXPECT_FALSE, and EXPECT_EQ_U64 macros for test assertions
- Supports both local and shared radix tree testing via TEST_SHARED_RT compilation flag
- Tests are parameterized to cover different node types, key shifts, and insertion patterns
- Validates that iteration returns keys in sorted order regardless of insertion order
- Performs comprehensive validation of radix tree consistency across all operations
- Uses TestValueType for value storage, with values typically equal to their corresponding keys

## Simplified Source

```c
static void test_basic(rt_node_class_test_elem *test_info, int shift, bool asc)
{
    MemoryContext radixtree_ctx;
    rt_radix_tree *radixtree;
    rt_iter *iter;
    uint64 *keys;
    int children = test_info->nkeys;

    // Create memory context and radix tree
    radixtree_ctx = AllocSetContextCreate(CurrentMemoryContext, "test_radix_tree", ALLOCSET_SMALL_SIZES);

    #ifdef TEST_SHARED_RT
        int tranche_id = LWLockNewTrancheId();
        dsa_area *dsa = dsa_create(tranche_id);
        LWLockRegisterTranche(tranche_id, "test_radix_tree");
        radixtree = rt_create(radixtree_ctx, dsa, tranche_id);
    #else
        radixtree = rt_create(radixtree_ctx);
    #endif

    elog(NOTICE, "testing node %s with shift %d and %s keys",
         test_info->class_name, shift, asc ? "ascending" : "descending");

    // Generate test keys with specified ordering
    keys = palloc(sizeof(uint64) * children);
    for (int i = 0; i < children; i++) {
        if (asc)
            keys[i] = (uint64) i << shift;
        else
            keys[i] = (uint64) (children - 1 - i) << shift;
    }

    // Test 1: Insert keys (should return false for new insertions)
    for (int i = 0; i < children; i++)
        EXPECT_FALSE(rt_set(radixtree, keys[i], (TestValueType *) &keys[i]));

    // Test 2: Lookup all keys
    for (int i = 0; i < children; i++) {
        TestValueType *value = rt_find(radixtree, keys[i]);
        EXPECT_TRUE(value != NULL);
        EXPECT_EQ_U64(*value, (TestValueType) keys[i]);
    }

    // Test 3: Update existing keys (should return true for existing)
    for (int i = 0; i < children; i++) {
        TestValueType update = keys[i] + 1;
        EXPECT_TRUE(rt_set(radixtree, keys[i], (TestValueType *) &update));
    }

    // Test 4: Delete and re-insert keys
    for (int i = 0; i < children; i++) {
        EXPECT_TRUE(rt_delete(radixtree, keys[i]));
        EXPECT_FALSE(rt_set(radixtree, keys[i], (TestValueType *) &keys[i]));
    }

    // Test 5: Verify lookups after re-insertion
    for (int i = 0; i < children; i++) {
        TestValueType *value = rt_find(radixtree, keys[i]);
        EXPECT_TRUE(value != NULL);
        EXPECT_EQ_U64(*value, (TestValueType) keys[i]);
    }

    // Test 6: Iteration (should return keys in sorted order)
    iter = rt_begin_iterate(radixtree);
    for (int i = 0; i < children; i++) {
        uint64 expected = asc ? keys[i] : keys[children - 1 - i];
        uint64 iterkey;
        TestValueType *iterval = rt_iterate_next(iter, &iterkey);

        EXPECT_TRUE(iterval != NULL);
        EXPECT_EQ_U64(iterkey, expected);
        EXPECT_EQ_U64(*iterval, expected);
    }
    rt_end_iterate(iter);

    // Test 7: Delete all keys and verify empty state
    for (int i = 0; i < children; i++)
        EXPECT_TRUE(rt_delete(radixtree, keys[i]));

    for (int i = 0; i < children; i++)
        EXPECT_TRUE(rt_find(radixtree, keys[i]) == NULL);

    // Cleanup
    rt_stats(radixtree);
    pfree(keys);
    rt_free(radixtree);

    #ifdef TEST_SHARED_RT
        dsa_detach(dsa);
    #endif
}
```