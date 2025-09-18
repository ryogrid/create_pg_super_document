# RegisterRelcacheInvalidation

## Location
[src/backend/utils/cache/inval.c:571-600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L571-L600)

## Overview
Registers a relcache invalidation event for a specific relation, ensuring relation cache entries are marked for invalidation and handles special cases for cached init files.

## Definition
```c
static void RegisterRelcacheInvalidation(Oid dbId, Oid relId)
```

## Detailed Description
RegisterRelcacheInvalidation is a static function that registers a relation cache invalidation message for a specific relation. Beyond simply adding the invalidation message, it performs several important additional tasks:

1. Calls GetCurrentCommandId(true) to ensure the next CommandCounterIncrement will trigger CommandEndInvalidationMessages(), which is necessary because relcache invalidation can occur outside of typical system catalog updates.

2. Manages the relcache init file invalidation flag. If the relation being invalidated is cached in a relcache init file, or if invalidating the whole relcache (relId == InvalidOid), it marks that the init file needs to be removed at commit time.

The function is part of PostgreSQL's cache invalidation system and ensures that relation cache entries are properly invalidated when relations are modified.

## Parameters / Member Variables
- `dbId`: Database OID where the relation resides (InvalidOid for shared relations)
- `relId`: OID of the relation being invalidated (InvalidOid to invalidate all relations)

## Dependencies
- Functions called/Symbols referenced:
  - [AddRelcacheInvalidationMessage](../A/AddRelcacheInvalidationMessage.md)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - [RelationIdIsInInitFile](RelationIdIsInInitFile.md)
- Called from (representative examples):
  - [CacheInvalidateHeapTuple](../C/CacheInvalidateHeapTuple.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [CacheInvalidateRelcacheAll](../C/CacheInvalidateRelcacheAll.md)
  - [CacheInvalidateRelcacheByTuple](../C/CacheInvalidateRelcacheByTuple.md)

## Notes and Other Information
- This is a static function internal to the invalidation system
- Handles the special case of relations cached in init files, which require file-level invalidation
- Uses GetCurrentCommandId(true) as a "quick hack" to ensure proper command-end processing
- The RelcacheInitFileInval flag triggers physical removal of cached init files at transaction commit
- Database-specific invalidations also invalidate the shared init file for simplicity