# AddCatcacheInvalidationMessage

## Location
[src/backend/utils/cache/inval.c:396-423](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L396-L423)

## Overview
AddCatcacheInvalidationMessage is a static function that creates and adds a catalog cache invalidation message to an invalidation message group, specifically targeting entries in the system catalog cache.

## Definition
```c
static void AddCatcacheInvalidationMessage(InvalidationMsgsGroup *group, int id, uint32 hashValue, Oid dbId)
```

## Detailed Description
This function constructs a SharedInvalidationMessage specifically for catalog cache invalidation and adds it to the specified invalidation group's catalog cache subgroup (CatCacheMsgs). It populates the message with the catalog cache ID, database OID, and hash value that identify the specific cache entry to be invalidated. The function includes a Valgrind-specific memory initialization to prevent false warnings about undefined memory in the shared invalidation ring buffer, which is accessed by multiple processes.

The function ensures that the catalog cache ID fits within the int8 range by asserting it's less than CHAR_MAX, then constructs the invalidation message and delegates the actual addition to AddInvalidationMessage.

## Parameters / Member Variables
- `group`: Pointer to the InvalidationMsgsGroup where the catcache invalidation message will be added
- `id`: Integer identifier of the catalog cache to be invalidated (must be less than CHAR_MAX)
- `hashValue`: 32-bit hash value identifying the specific cache entry
- `dbId`: Object identifier (Oid) of the database containing the cached entry

## Dependencies
- Functions called/Symbols referenced:
  - [AddInvalidationMessage](AddInvalidationMessage.md) (to add the constructed message to the group)
  - VALGRIND_MAKE_MEM_DEFINED (for memory debugging support)
- Constants used:
  - CatCacheMsgs (subgroup identifier for catalog cache messages)
  - CHAR_MAX (for ID range validation)
- Data structures used:
  - [InvalidationMsgsGroup](../I/InvalidationMsgsGroup.md)
  - SharedInvalidationMessage
  - int8, uint32, Oid types
- Called from:
  - [RegisterCatcacheInvalidation](../R/RegisterCatcacheInvalidation.md)

## Notes and Other Information
- This is a static function, only accessible within the inval.c file
- The function includes Valgrind memory debugging support to prevent spurious warnings in multi-process shared memory scenarios
- Part of PostgreSQL's catalog cache invalidation system that ensures cache consistency when catalog data changes
- The ID parameter is cast to int8, limiting the number of possible catalog caches to 127
- The hash value and database ID together uniquely identify a specific cached catalog entry to invalidate