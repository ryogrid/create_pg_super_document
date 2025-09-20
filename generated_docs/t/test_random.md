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
  - pg_prng_seed, pg_prng_uint64 (random number generation)
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
  - LWLockNewTrancheId, LWLockRegisterTranche, dsa_create, dsa_detach (shared memory operations)
- Called from (representative examples):
  - test_radixtree (main test function)

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