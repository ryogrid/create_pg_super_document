# CatalogCacheFlushCatalog

## Location
[src/backend/utils/cache/catcache.c:834-866](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L834-L866)

## Overview
Flushes all catalog cache entries that came from a specified system catalog, typically needed after VACUUM FULL or CLUSTER operations.

## Definition
```c
void CatalogCacheFlushCatalog(Oid catId)
```

## Detailed Description
CatalogCacheFlushCatalog selectively flushes catalog caches that contain tuples from a specific system catalog identified by its OID. This function is essential after operations like VACUUM FULL or CLUSTER on system catalogs, because these operations can change the physical storage locations (TIDs) of tuples, making cached references invalid.

The function iterates through all catalog caches and checks each cache's cc_reloid to determine if it stores tuples from the target catalog. When a match is found, it resets that cache completely and triggers system cache callbacks to notify other components of the invalidation.

The design avoids re-initializing cache structures during flush operations to prevent complications that arise when cache flushes occur while cache entries are being loaded.

## Parameters / Member Variables
- `catId`: The OID of the system catalog whose cached entries should be flushed

## Dependencies
- Functions called/Symbols referenced:
  - [ResetCatalogCache](../R/ResetCatalogCache.md)
  - [CallSyscacheCallbacks](CallSyscacheCallbacks.md)
  - slist_foreach
  - slist_container
  - CACHE_elog
  - DEBUG2
- Called from (representative examples):
  - [LocalExecuteInvalidationMessage](../L/LocalExecuteInvalidationMessage.md)
  - Referenced in CatCacheHeader

## Notes and Other Information
- Critical for maintaining cache consistency after physical reorganization of system catalogs
- Only flushes caches that actually contain tuples from the specified catalog (selective flushing)
- Triggers syscache callbacks to ensure other components are notified of the invalidation
- Includes debug logging to help trace catalog-specific cache flushes
- Avoids cache re-initialization during flush to prevent loading-related complications
- The function assumes that tuple descriptors of cacheable system tables do not change