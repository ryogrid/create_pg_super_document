# record_type_typmod_hash

## Location
[src/backend/utils/cache/typcache.c:1926-1936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1926-L1936)

## Overview
A hash function used by the hash table that stores RecordCacheEntry structures, providing efficient lookups for record type typmod assignments.

## Definition

```c
static uint32
record_type_typmod_hash(const void *data, size_t size)
```
## Detailed Description
This function serves as the hash function for PostgreSQL's internal hash table that manages RecordCacheEntry structures. It extracts the TupleDesc from a RecordCacheEntry and delegates to hashRowType() to compute a hash value based on the row type's structure.

The function is designed to work with PostgreSQL's hash table infrastructure, following the standard hash function signature expected by the hash table implementation. It ensures that RecordCacheEntry objects with equivalent TupleDesc structures will hash to the same value, enabling efficient retrieval and avoiding duplicate entries in the cache.

## Parameters / Member Variables
- : Pointer to the RecordCacheEntry structure to be hashed
- : Size parameter (required by hash function interface but not used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [hashRowType](../h/hashRowType.md)
  - [RecordCacheEntry](../R/RecordCacheEntry.md) (struct type)
- Called from (representative examples):
  - [assign_record_type_typmod](../a/assign_record_type_typmod.md) (src/backend/utils/cache/typcache.c:1969)

## Notes and Other Information
- This is a static function internal to typcache.c, not exposed to external modules
- The hash value computation is delegated to hashRowType(), which ensures consistency with row type equality semantics
- The size parameter is unused, following the common pattern in PostgreSQL hash functions where the data structure itself determines the hash computation
- Part of PostgreSQL's record type caching mechanism that assigns unique typmod values to anonymous record types