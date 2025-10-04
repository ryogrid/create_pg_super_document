# test_random

## Location
[src/test/modules/test_radixtree/test_radixtree.c:305-445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_radixtree/test_radixtree.c#L305-L445)

## Overview
A comprehensive stress test function that validates radix tree operations using randomly generated keys, testing insertion, lookup, iteration, and deletion with a large dataset.

## Definition
```c
static void test_random(void)
```

## Detailed Description
This function performs extensive stress testing of radix tree functionality using 100,000 randomly generated keys. The test sequence includes:
1. Generates random keys using a PRNG with timestamp-based seed and applies a filter to limit memory usage
2. Inserts all keys with corresponding values into the radix tree
3. Validates that all inserted keys can be found with correct values
4. Sorts keys and tests for absence of non-existent keys between, below, and above the key range
5. Tests iteration functionality with proper ordering and duplicate handling
6. Deletes all keys in the original random order and verifies the tree is empty
The function uses deterministic randomness by reseeding the PRNG for deletion, ensuring reproducible test results.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (seed generation)
  - [pg_prng_seed](../p/pg_prng_seed.md), pg_prng_uint64 (random number generation)
  - AllocSetContextCreate (memory context management)
  - rt_create (radix tree creation)
  - rt_set (key insertion)
  - rt_find (key lookup operations)
  - rt_delete (key deletion)
  - [rt_num_entries](../r/rt_num_entries.md) (tree size verification)
  - rt_begin_iterate, rt_iterate_next, rt_end_iterate (iteration operations)
  - rt_stats (statistics reporting)
  - rt_free (radix tree cleanup)
  - qsort (key sorting for validation)
  - [key_cmp](../k/key_cmp.md) (comparison function for sorting)
  - [palloc](../p/palloc.md), pfree (memory allocation/deallocation)
  - [LWLockNewTrancheId](../L/LWLockNewTrancheId.md), LWLockRegisterTranche, dsa_create, dsa_detach (shared memory operations)
- Called from (representative examples):
  - [test_radixtree](test_radixtree.md) (main test function)

## Notes and Other Information
- This is a static function used internally within the test_radixtree module
- Uses EXPECT_TRUE and EXPECT_EQ_U64 macros for test assertions
- Supports both local and shared radix tree testing via TEST_SHARED_RT compilation flag
- Uses a memory-limiting filter (0x07FF00FF) to constrain the key space and control memory usage
- Tests 100,000 keys by default, providing substantial stress testing coverage
- Handles duplicate keys correctly during iteration by skipping them
- Validates negative cases (keys that should not be found) to ensure tree integrity
- Uses deterministic randomness to ensure reproducible test results across runs
- Verifies tree is completely empty after all deletions, ensuring proper cleanup

## Simplified Source

```c
static void test_random(void)
{
    MemoryContext radixtree_ctx;
    rt_radix_tree *radixtree;
    rt_iter *iter;
    pg_prng_state state;

    // Setup: limit key space and prepare random seed
    uint64 filter = ((uint64) (0x07 << 24) | (0xFF << 16) | 0xFF);
    uint64 seed = GetCurrentTimestamp();
    int num_keys = 100000;
    uint64 *keys;

    // Create memory context and radix tree
    radixtree_ctx = AllocSetContextCreate(CurrentMemoryContext,
                                          "test_radix_tree",
                                          ALLOCSET_SMALL_SIZES);

#ifdef TEST_SHARED_RT
    // Shared radix tree setup
    int tranche_id = LWLockNewTrancheId();
    dsa_area *dsa = dsa_create(tranche_id);
    LWLockRegisterTranche(tranche_id, "test_radix_tree");
    radixtree = rt_create(radixtree_ctx, dsa, tranche_id);
#else
    radixtree = rt_create(radixtree_ctx);
#endif

    // Phase 1: Insert random keys
    pg_prng_seed(&state, seed);
    keys = (TestValueType *) palloc(sizeof(uint64) * num_keys);
    for (uint64 i = 0; i < num_keys; i++)
    {
        uint64 key = pg_prng_uint64(&state) & filter;
        TestValueType val = (TestValueType) key;

        keys[i] = key;
        rt_set(radixtree, key, &val);
    }

    rt_stats(radixtree);

    // Phase 2: Verify all inserted keys can be found
    for (uint64 i = 0; i < num_keys; i++)
    {
        TestValueType *value = rt_find(radixtree, keys[i]);
        EXPECT_TRUE(value != NULL);
        EXPECT_EQ_U64(*value, keys[i]);
    }

    // Phase 3: Test absence of non-existent keys
    qsort(keys, num_keys, sizeof(uint64), key_cmp);

    // Test keys between existing ones
    for (uint64 i = 0; i < num_keys - 1; i++)
    {
        if (keys[i + 1] == keys[i] || keys[i + 1] == keys[i] + 1)
            continue;

        TestValueType *value = rt_find(radixtree, keys[i] + 1);
        EXPECT_TRUE(value == NULL);
    }

    // Test keys below and above range
    for (uint64 key = 0; key < keys[0] && key <= 10000; key++)
    {
        TestValueType *value = rt_find(radixtree, key);
        EXPECT_TRUE(value == NULL);
    }

    for (uint64 i = 1; i < 10000; i++)
    {
        TestValueType *value = rt_find(radixtree, keys[num_keys - 1] + i);
        EXPECT_TRUE(value == NULL);
    }

    // Phase 4: Test iteration
    iter = rt_begin_iterate(radixtree);
    for (int i = 0; i < num_keys; i++)
    {
        // Skip duplicate keys
        if (i < num_keys - 1 && keys[i + 1] == keys[i])
            continue;

        uint64 expected = keys[i];
        uint64 iterkey;
        TestValueType *iterval = rt_iterate_next(iter, &iterkey);

        EXPECT_TRUE(iterval != NULL);
        EXPECT_EQ_U64(iterkey, expected);
        EXPECT_EQ_U64(*iterval, expected);
    }
    rt_end_iterate(iter);

    // Phase 5: Delete all keys in original random order
    pg_prng_seed(&state, seed);
    for (uint64 i = 0; i < num_keys; i++)
    {
        uint64 key = pg_prng_uint64(&state) & filter;
        rt_delete(radixtree, key);
    }

    // Verify tree is empty
    EXPECT_TRUE(rt_num_entries(radixtree) == 0);

    // Cleanup
    pfree(keys);
    rt_free(radixtree);

#ifdef TEST_SHARED_RT
    dsa_detach(dsa);
#endif
}
```