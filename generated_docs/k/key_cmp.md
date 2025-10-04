# key_cmp

## Location
[src/test/modules/test_radixtree/test_radixtree.c:299-304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_radixtree/test_radixtree.c#L299-L304)

## Overview
A comparison function for sorting uint64 keys, designed to be used with standard library sorting functions like qsort.

## Definition
```c
static int key_cmp(const void *a, const void *b)
```

## Detailed Description
This function provides a comparison interface for uint64 values that conforms to the standard C library qsort comparison function signature. It dereferences the void pointers to uint64 values and uses PostgreSQL's pg_cmp_u64 utility function to perform the actual comparison. The function returns a negative value if the first key is less than the second, zero if they are equal, and a positive value if the first key is greater than the second.

## Parameters / Member Variables
- `a`: Pointer to the first uint64 key to compare (cast from const void *)
- `b`: Pointer to the second uint64 key to compare (cast from const void *)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cmp_u64](../p/pg_cmp_u64.md) (PostgreSQL's uint64 comparison utility function)
- Called from (representative examples):
  - [test_random](../t/test_random.md) (used for sorting keys in random testing scenarios)

## Notes and Other Information
- This is a static function used internally within the test_radixtree module
- Follows the standard C library comparison function contract for use with qsort
- Returns int values: negative (<0), zero (==0), or positive (>0) based on comparison result
- Assumes both input pointers point to valid uint64 values
- Used primarily in test scenarios where keys need to be sorted for validation purposes

## Simplified Source

```c
static int key_cmp(const void *a, const void *b)
{
    return pg_cmp_u64(*(const uint64 *) a, *(const uint64 *) b);
}
```