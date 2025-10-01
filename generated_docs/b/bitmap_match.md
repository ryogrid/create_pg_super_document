# bitmap_match

## Location
[src/backend/nodes/bitmapset.c:1442-1447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L1442-L1447)

## Overview
The `bitmap_match` function serves as a key comparison function for hash tables that use Bitmapset pointers as keys, complementing the `bitmap_hash` function.

## Definition
```c
int bitmap_match(const void *key1, const void *key2, Size keysize)
```

## Detailed Description
This function implements the key comparison logic required by PostgreSQL's hash table infrastructure when using Bitmapsets as hash table keys. It follows the standard hash table match function interface, taking two generic key pointers and comparing the Bitmapsets they point to.

The function performs pointer dereferencing to extract the actual Bitmapsets from the key pointers, then uses `bms_equal` to test for equality. Following PostgreSQL's hash table conventions, it returns 0 when the keys match (are equal) and non-zero when they don't match.

The logic inverts the result of `bms_equal` because PostgreSQL's hash table implementation expects match functions to return 0 for matching keys, while `bms_equal` returns 1 for equal Bitmapsets.

## Parameters
- `key1`: Pointer to the first key (expected to be a `Bitmapset **`)
- `key2`: Pointer to the second key (expected to be a `Bitmapset **`)
- `keysize`: Size of the key data (must equal `sizeof(Bitmapset *)`)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_equal](bms_equal.md)
- Called from (examples):
  - [build_join_rel_hash](build_join_rel_hash.md)
  - bms_is_empty (header usage)

## Notes and Other Information
- Must be used together with `bitmap_hash` for complete hash table functionality
- Returns 0 for equal Bitmapsets, non-zero for unequal ones (inverted from `bms_equal`)
- The function expects keys to be pointers to Bitmapset pointers (double indirection)
- Includes runtime assertion to verify correct key size usage
- Essential component for using Bitmapsets as hash table keys in PostgreSQL's hash table infrastructure
- The return value semantics follow PostgreSQL's hash table match function conventions

## Simplified Source

```c
int
bitmap_match(const void *key1, const void *key2, Size keysize)
{
    // Verify we're dealing with Bitmapset pointers
    Assert(keysize == sizeof(Bitmapset *));

    // Extract Bitmapsets and compare for equality
    // Return 0 for match, non-zero for non-match (inverted from bms_equal)
    return !bms_equal(*((const Bitmapset *const *) key1),
                      *((const Bitmapset *const *) key2));
}
```