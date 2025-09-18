# missing_match

## Location
src/backend/access/common/heaptuple.c: 108 - 121

## Overview
The `missing_match` function is a comparison function used by the missing attribute cache hash table to determine key equality during hash table operations in PostgreSQL heap tuple processing.

## Definition
```c
static int missing_match(const void *key1, const void *key2, Size keysize)
```

## Detailed Description
This function serves as a key comparison callback for the missing attribute cache hash table. It implements a three-way comparison between two `missing_cache_key` structures, first comparing their lengths and then performing a byte-wise comparison of their values. The function returns an integer indicating the relative ordering of the keys, following standard comparison function conventions where 0 indicates equality, positive values indicate key1 > key2, and negative values indicate key1 < key2.

## Parameters / Member Variables
- `key1`: Pointer to the first `missing_cache_key` structure to compare
- `key2`: Pointer to the second `missing_cache_key` structure to compare  
- `keysize`: Size parameter (unused in this implementation, as sizes come from the cache key structures)

## Dependencies
- Functions called/Symbols referenced:
  - `missing_cache_key`: Structure type containing value and length fields
  - `DatumGetPointer`: Macro to extract pointer from Datum value
  - `memcmp`: Standard C library function for memory comparison
- Called from (representative examples):
  - `init_missing_cache`: Used as match function callback when initializing the missing attribute cache

## Notes and Other Information
- This is a static function with internal linkage within heaptuple.c
- The function ignores the `keysize` parameter, using lengths from the cache key structures instead
- Implements lexicographic ordering: first by length, then by byte content
- Returns 0 for equal keys, positive for key1 > key2, negative for key1 < key2
- Part of PostgreSQL missing attribute cache infrastructure for efficient tuple processing