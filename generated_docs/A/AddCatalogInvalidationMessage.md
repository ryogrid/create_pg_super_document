# AddCatalogInvalidationMessage

## Location
src/backend/utils/cache/inval.c: 424 - 441

## Overview
AddCatalogInvalidationMessage is a static function that creates and adds a whole-catalog invalidation message to an invalidation message group, targeting all cached entries for a specific system catalog.

## Definition
```c
static void AddCatalogInvalidationMessage(InvalidationMsgsGroup *group, Oid dbId, Oid catId)
```

## Detailed Description
This function constructs a SharedInvalidationMessage for whole-catalog invalidation, which invalidates all cached entries related to a specific system catalog rather than individual cache entries. It sets the message type to SHAREDINVALCATALOG_ID and populates it with the database OID and catalog OID to identify which catalog's cache entries should be invalidated. Like AddCatcacheInvalidationMessage, it includes Valgrind memory initialization to prevent false warnings in the shared invalidation ring buffer accessed by multiple processes.

This type of invalidation is more aggressive than individual cache entry invalidation, clearing all cache entries for an entire catalog at once, typically used when structural changes occur to a catalog.

## Parameters / Member Variables
- `group`: Pointer to the InvalidationMsgsGroup where the catalog invalidation message will be added
- `dbId`: Object identifier (Oid) of the database containing the catalog to be invalidated
- `catId`: Object identifier (Oid) of the specific system catalog to be invalidated

## Dependencies
- Functions called/Symbols referenced:
  - [AddInvalidationMessage](AddInvalidationMessage.md) (to add the constructed message to the group)
  - VALGRIND_MAKE_MEM_DEFINED (for memory debugging support)
- Constants used:
  - SHAREDINVALCATALOG_ID (message type identifier for catalog invalidation)
  - CatCacheMsgs (subgroup identifier for catalog cache messages)
- Data structures used:
  - [InvalidationMsgsGroup](../I/InvalidationMsgsGroup.md)
  - SharedInvalidationMessage
  - Oid type
- Called from:
  - [RegisterCatalogInvalidation](../R/RegisterCatalogInvalidation.md)

## Notes and Other Information
- This is a static function, only accessible within the inval.c file
- Performs whole-catalog invalidation rather than individual cache entry invalidation
- Uses the same CatCacheMsgs subgroup as individual catalog cache invalidations
- Includes Valgrind memory debugging support for multi-process shared memory scenarios
- Part of PostgreSQL's catalog cache invalidation system for handling structural catalog changes
- More aggressive than AddCatcacheInvalidationMessage as it invalidates entire catalogs rather than specific entries