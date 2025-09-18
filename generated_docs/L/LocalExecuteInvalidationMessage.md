# LocalExecuteInvalidationMessage

## Location
src/backend/utils/cache/inval.c: 706 - 792

## Overview
Processes a single invalidation message to flush local caches based on the message type, without transmitting the message to other backends.

## Definition


## Detailed Description
LocalExecuteInvalidationMessage is the core function that executes invalidation messages locally within a single backend process. It examines the message ID to determine the type of invalidation required and performs the appropriate cache invalidation operations. The function handles six different types of invalidation messages:

1. **System cache invalidation** (id >= 0): Invalidates specific system cache entries
2. **Catalog cache invalidation** (SHAREDINVALCATALOG_ID): Flushes entire catalog cache categories  
3. **Relation cache invalidation** (SHAREDINVALRELCACHE_ID): Invalidates relation cache entries
4. **Storage manager invalidation** (SHAREDINVALSNGR_ID): Releases storage manager cache entries
5. **Relation map invalidation** (SHAREDINVALRELMAP_ID): Invalidates relation mapping cache
6. **Snapshot invalidation** (SHAREDINVALSNAPSHOT_ID): Invalidates catalog snapshots

Each invalidation type checks database IDs to ensure only relevant cache entries are invalidated, improving performance by avoiding unnecessary work for other databases.

## Parameters / Member Variables
- : Pointer to SharedInvalidationMessage containing the invalidation details including message ID, database ID, and type-specific data (hash values, relation IDs, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [InvalidateCatalogSnapshot](../I/InvalidateCatalogSnapshot.md)
  - [SysCacheInvalidate](../S/SysCacheInvalidate.md)
  - [CallSyscacheCallbacks](../C/CallSyscacheCallbacks.md)
  - [CatalogCacheFlushCatalog](../C/CatalogCacheFlushCatalog.md)
  - [RelationCacheInvalidate](../R/RelationCacheInvalidate.md)
  - [RelationCacheInvalidateEntry](../R/RelationCacheInvalidateEntry.md)
  - [smgrreleaserellocator](../s/smgrreleaserellocator.md)
  - [RelationMapInvalidate](../R/RelationMapInvalidate.md)
- Called from (representative examples):
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - [AtEOXact_Inval](../A/AtEOXact_Inval.md)
  - [AtEOSubXact_Inval](../A/AtEOSubXact_Inval.md)
  - [CommandEndInvalidationMessages](../C/CommandEndInvalidationMessages.md)
  - [ReorderBufferExecuteInvalidations](../R/ReorderBufferExecuteInvalidations.md)

## Notes and Other Information
- The function only affects local caches and does not transmit messages to other backends
- Database ID checking (MyDatabaseId vs InvalidOid) ensures cross-database invalidations are handled correctly
- InvalidOid database ID indicates shared catalogs that affect all databases
- The function includes callback mechanisms for both syscache and relcache invalidations
- Fatal error is triggered for unrecognized message IDs to ensure system integrity