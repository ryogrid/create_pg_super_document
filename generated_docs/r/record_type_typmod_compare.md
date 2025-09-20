# record_type_typmod_compare

## Location
[src/backend/utils/cache/typcache.c:1937-1952](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1937-L1952)

## Overview
A comparison function used by the hash table that stores RecordCacheEntry structures, determining equality between two record cache entries based on their TupleDesc structures.

## Definition

```c
static int
record_type_typmod_compare(const void *a, const void *b, size_t size)
```
## Detailed Description
This function serves as the key comparison function for PostgreSQL's internal hash table that manages RecordCacheEntry structures. It extracts the TupleDesc from both RecordCacheEntry parameters and delegates to equalRowTypes() to determine if they represent equivalent row types.

The function follows the standard comparison function interface required by PostgreSQL's hash table implementation, returning 0 for equality and 1 for inequality. This ensures that RecordCacheEntry objects with structurally identical TupleDesc definitions are treated as equivalent keys in the hash table, preventing duplicate cache entries and enabling proper cache lookup functionality.

## Parameters / Member Variables
- : Pointer to the first RecordCacheEntry structure to compare
- : Pointer to the second RecordCacheEntry structure to compare  
- : Size parameter (required by comparison function interface but not used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [equalRowTypes](../e/equalRowTypes.md)
  - [RecordCacheEntry](../R/RecordCacheEntry.md) (struct type)
- Called from (representative examples):
  - [assign_record_type_typmod](../a/assign_record_type_typmod.md) (src/backend/utils/cache/typcache.c:1970)

## Notes and Other Information
- This is a static function internal to typcache.c, not exposed to external modules
- Returns 0 when TupleDesc structures are equal, 1 when they differ (standard comparison function semantics)
- The size parameter is unused, following the common pattern in PostgreSQL comparison functions
- Works in conjunction with record_type_typmod_hash() to provide complete hash table key functionality
- Part of PostgreSQL's record type caching mechanism that assigns unique typmod values to anonymous record types