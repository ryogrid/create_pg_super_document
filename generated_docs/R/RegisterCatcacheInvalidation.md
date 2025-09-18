# RegisterCatcacheInvalidation

## Location
src/backend/utils/cache/inval.c: 545 - 558

## Overview
Registers an invalidation event for a catalog cache (catcache) tuple entry by adding it to the current command's invalidation message group.

## Definition
```c
static void RegisterCatcacheInvalidation(int cacheId, uint32 hashValue, Oid dbId)
```

## Detailed Description
This function creates and registers a catalog cache invalidation message for a specific tuple entry. It serves as a higher-level interface to AddCatcacheInvalidationMessage, specifically targeting the current command's invalidation message group within the current transaction context. The function is part of PostgreSQL's cache invalidation system, ensuring that changes to catalog tables are properly communicated to invalidate stale cache entries.

When called, the function adds the invalidation message to the CurrentCmdInvalidMsgs group of the current transaction's invalidation info structure. This ensures that the invalidation will be processed at the appropriate time during transaction processing (typically at command end or transaction commit).

## Parameters / Member Variables
- `cacheId`: Integer identifier of the catalog cache that needs invalidation (must be less than CHAR_MAX)
- `hashValue`: Hash value of the specific tuple being invalidated, used for efficient cache lookup
- `dbId`: Database OID where the tuple resides (0 for shared catalogs)

## Dependencies
- Functions called/Symbols referenced:
  - AddCatcacheInvalidationMessage (creates and adds the actual invalidation message)
- Global variables referenced:
  - transInvalInfo (current transaction's invalidation information structure)
- Types referenced:
  - TransInvalidationInfo (transaction-level invalidation context)
  - InvalidationMsgsGroup (message group for organizing invalidations)
- Called from:
  - CacheInvalidateHeapTuple (when heap tuple changes require cache invalidation)

## Notes and Other Information
- This is a static function, only accessible within the inval.c module
- Part of PostgreSQL's transactional invalidation system for maintaining cache coherency
- The function assumes transInvalInfo is properly initialized (transaction is active)
- Invalidation messages are organized by transaction and command boundaries for proper rollback semantics
- The cacheId parameter corresponds to specific catalog caches (pg_class, pg_attribute, etc.)
- Hash values enable efficient lookup and invalidation of specific cache entries rather than wholesale cache clearing