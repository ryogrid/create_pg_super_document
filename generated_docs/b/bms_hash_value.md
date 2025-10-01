# bms_hash_value

## Location
[src/backend/nodes/bitmapset.c:1416-1431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L1416-L1431)

## Overview
The `bms_hash_value` function computes a hash key for a Bitmapset, enabling Bitmapsets to be used in hash tables and hash-based operations.

## Definition
```c
uint32 bms_hash_value(const Bitmapset *a)
```

## Detailed Description
This function generates a 32-bit hash value for a given Bitmapset by hashing the underlying bitmap word array. The implementation uses PostgreSQL's standard `hash_any` function to compute the hash over the raw bitmap data. 

The function handles the special case of NULL or empty Bitmapsets by returning a hash value of 0, ensuring that all empty sets hash to the same value regardless of their internal representation.

The hash is computed over the entire `words` array of the Bitmapset, taking into account the number of words (`nwords`) to ensure that different-sized Bitmapsets with the same set bits produce different hash values when appropriate.

## Parameters
- `a`: Pointer to the Bitmapset to hash (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md)
  - [hash_any](../h/hash_any.md)
  - [DatumGetUInt32](../D/DatumGetUInt32.md)
  - bitmapword (type)
- Called from (examples):
  - [bitmap_hash](bitmap_hash.md)
  - bms_is_empty (header usage)

## Notes and Other Information
- Returns 0 for all NULL/empty Bitmapsets to ensure consistent hashing
- Uses the same hash function (`hash_any`) as other PostgreSQL data types for consistency
- The hash computation includes the full bitmap word array, not just the set bits
- Essential for using Bitmapsets as keys in hash tables or for hash-based equality comparisons
- The function assumes the Bitmapset is in canonical form (no trailing zero words)

## Simplified Source

```c
uint32
bms_hash_value(const Bitmapset *a)
{
    // Validate input and handle NULL/empty case
    if (a == NULL)
        return 0;  // All empty sets hash to 0

    // Hash the bitmap word array
    return DatumGetUInt32(hash_any((const unsigned char *) a->words,
                                   a->nwords * sizeof(bitmapword)));
}
```