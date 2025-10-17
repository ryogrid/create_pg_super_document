# element_match

## Location
[src/backend/utils/adt/array_typanalyze.c:725-739](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_typanalyze.c#L725-L739)

## Overview
The element_match function serves as a matching function for hash table lookups, determining whether two array element keys are equal.

## Definition
```c
static int element_match(const void *key1, const void *key2, Size keysize)
```

## Detailed Description
This function acts as a comparison callback for hash table operations, specifically for matching keys during hash table lookups in array statistics computation. It delegates the actual comparison to the element_compare function, which uses the element type's default comparison function and appropriate collation to determine equality. The function returns 0 when elements are equal (match) and non-zero when they differ, following standard C comparison function conventions required by PostgreSQL's hash table implementation.

## Parameters / Member Variables
- `key1`: Pointer to the first Datum key to compare
- `key2`: Pointer to the second Datum key to compare  
- `keysize`: Size of the keys (unused but required by hash table interface)

## Dependencies
- Functions called/Symbols referenced:
  - [element_compare](element_compare.md) (which uses array_extra_data->cmp and collation)
- Called from (representative examples):
  - [compute_array_stats](../c/compute_array_stats.md) (registered as hash table match function)

## Notes and Other Information
- Serves as a thin wrapper around element_compare for hash table compatibility
- The keysize parameter is explicitly noted as superfluous but required by the hash table API
- Returns 0 for matching elements, non-zero for different elements (standard comparison semantics)
- Uses the element type's comparison procedure and collation for accurate equality testing
- Essential for proper hash table key lookup during array element frequency tracking
- Located in src/backend/utils/adt/array_typanalyze.c:725-739

## Simplified Source

```c
static int element_match(const void *key1, const void *key2, Size keysize) {
    // Delegate to element_compare for actual comparison
    // Returns 0 for match, non-zero for different elements
    return element_compare(key1, key2);
}
```