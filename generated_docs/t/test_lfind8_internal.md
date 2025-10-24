# test_lfind8_internal

## Location
[src/test/modules/test_lfind/test_lfind.c:30-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_lfind/test_lfind.c#L30-L58)

## Overview
A static helper function that serves as the workhorse for test_lfind8, performing comprehensive testing of the pg_lfind8 function with various search scenarios and boundary conditions.

## Definition

```c
static void
test_lfind8_internal(uint8 key)
```
## Detailed Description
This function thoroughly tests the pg_lfind8 function by creating test buffers filled with 0xFF values and placing the target key at specific positions. It performs two main test scenarios:

1. **Tail search testing**: Places the key at the tail position (len_with_tail - 1) to test the one-byte-at-a-time search path that occurs when the buffer size includes a tail portion.

2. **Vector operations testing**: Places the key at the vector boundary position (len_no_tail - 1) to test the vectorized search operations when the buffer size is aligned for SIMD operations.

For each scenario, it performs three boundary tests:
- Searches for (key - 1) and expects it NOT to be found
- Searches for the actual key and expects it to be found  
- Searches for (key + 1) and expects it NOT to be found

The function uses elog(ERROR, ...) to report any test failures, which will abort the test execution.

## Parameters / Member Variables
- `key`: The 8-bit unsigned integer value to use as the target for search testing. This key is placed in the test buffer and searched for, along with its adjacent values (key±1) for boundary testing.
## Dependencies
- Functions called/Symbols referenced:
  - LEN_WITH_TAIL (macro)
  - LEN_NO_TAIL (macro) 
  - Vector8 (type)
  - [pg_lfind8](../p/pg_lfind8.md) (primary function being tested)
  - memset (standard C library function)
  - elog (PostgreSQL logging function)

- Called from:
  - [test_lfind8](test_lfind8.md) (8 different calls with various key values)

## Notes and Other Information
- This is a static function, only accessible within the test_lfind.c file
- The function tests both vectorized and non-vectorized code paths of pg_lfind8
- Uses 0xFF as fill value to ensure the target key stands out in the buffer
- Boundary testing with key±1 helps verify that the search function doesn't have off-by-one errors or false positives
- The Vector8 type and associated macros indicate this is testing 8-byte vectorized search operations
- Part of PostgreSQL's test module infrastructure for validating linear search functionality

## Simplified Source

```c
static void test_lfind8_internal(uint8 key) {
    uint8 charbuf[LEN_WITH_TAIL(Vector8)];
    const int len_no_tail = LEN_NO_TAIL(Vector8);
    const int len_with_tail = LEN_WITH_TAIL(Vector8);

    // Test 1: Tail search (one-byte-at-a-time path)
    memset(charbuf, 0xFF, len_with_tail);
    charbuf[len_with_tail - 1] = key;

    // Test boundary conditions: key-1 should not exist, key should exist, key+1 should not exist
    if (key > 0x00 && pg_lfind8(key - 1, charbuf, len_with_tail))
        elog(ERROR, "pg_lfind8() found nonexistent element '0x%x'", key - 1);
    if (key < 0xFF && !pg_lfind8(key, charbuf, len_with_tail))
        elog(ERROR, "pg_lfind8() did not find existing element '0x%x'", key);
    if (key < 0xFE && pg_lfind8(key + 1, charbuf, len_with_tail))
        elog(ERROR, "pg_lfind8() found nonexistent element '0x%x'", key + 1);

    // Test 2: Vector operations (SIMD path)
    memset(charbuf, 0xFF, len_with_tail);
    charbuf[len_no_tail - 1] = key;

    // Same boundary tests for vectorized path
    if (key > 0x00 && pg_lfind8(key - 1, charbuf, len_no_tail))
        elog(ERROR, "pg_lfind8() found nonexistent element '0x%x'", key - 1);
    if (key < 0xFF && !pg_lfind8(key, charbuf, len_no_tail))
        elog(ERROR, "pg_lfind8() did not find existing element '0x%x'", key);
    if (key < 0xFE && pg_lfind8(key + 1, charbuf, len_no_tail))
        elog(ERROR, "pg_lfind8() found nonexistent element '0x%x'", key + 1);
}
```