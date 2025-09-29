# LocalExecuteInvalidationMessage

## Location
[src/backend/utils/cache/inval.c:706-792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L706-L792)

## Overview
Processes a single invalidation message to flush local caches based on the message type, without transmitting the message to other backends.

## Definition

```c
struct RELCACHECALLBACK *ccitem = relcache_callback_list + i;
```
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

## Simplified Source

```c
// Simplified version of LocalExecuteInvalidationMessage
void LocalExecuteInvalidationMessage(SharedInvalidationMessage *msg) {
    // Handle system cache invalidation (positive IDs)
    if (msg->id >= 0) {
        if (msg->cc.dbId == MyDatabaseId || msg->cc.dbId == InvalidOid) {
            InvalidateCatalogSnapshot();
            SysCacheInvalidate(msg->cc.id, msg->cc.hashValue);
            CallSyscacheCallbacks(msg->cc.id, msg->cc.hashValue);
        }
    }
    // Handle catalog cache flush
    else if (msg->id == SHAREDINVALCATALOG_ID) {
        if (msg->cat.dbId == MyDatabaseId || msg->cat.dbId == InvalidOid) {
            InvalidateCatalogSnapshot();
            CatalogCacheFlushCatalog(msg->cat.catId);
        }
    }
    // Handle relation cache invalidation
    else if (msg->id == SHAREDINVALRELCACHE_ID) {
        if (msg->rc.dbId == MyDatabaseId || msg->rc.dbId == InvalidOid) {
            // Invalidate all relations or specific relation
            if (msg->rc.relId == InvalidOid)
                RelationCacheInvalidate(false);
            else
                RelationCacheInvalidateEntry(msg->rc.relId);

            // Execute relation cache callbacks
            for (int i = 0; i < relcache_callback_count; i++) {
                struct RELCACHECALLBACK *ccitem = relcache_callback_list + i;
                ccitem->function(ccitem->arg, msg->rc.relId);
            }
        }
    }
    // Handle storage manager invalidation
    else if (msg->id == SHAREDINVALSMGR_ID) {
        // Build file locator and release storage manager entry
        RelFileLocatorBackend rlocator;
        rlocator.locator = msg->sm.rlocator;
        rlocator.backend = (msg->sm.backend_hi << 16) | (int) msg->sm.backend_lo;
        smgrreleaserellocator(rlocator);
    }
    // Handle relation mapping invalidation
    else if (msg->id == SHAREDINVALRELMAP_ID) {
        if (msg->rm.dbId == InvalidOid)
            RelationMapInvalidate(true);  // Shared catalogs
        else if (msg->rm.dbId == MyDatabaseId)
            RelationMapInvalidate(false); // Local database
    }
    // Handle snapshot invalidation
    else if (msg->id == SHAREDINVALSNAPSHOT_ID) {
        if (msg->sn.dbId == InvalidOid || msg->sn.dbId == MyDatabaseId)
            InvalidateCatalogSnapshot();
    }
    // Unknown message type
    else {
        elog(FATAL, "unrecognized SI message ID: %d", msg->id);
    }
}
```

Key simplifications made:
- Consolidated database ID checks into more readable conditions
- Added descriptive comments for each invalidation type
- Simplified variable declarations by moving them closer to usage
- Streamlined the storage manager backend reconstruction logic
- Combined similar snapshot invalidation conditions
- Maintained all essential logic while improving readability