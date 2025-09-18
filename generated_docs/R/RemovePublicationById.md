# RemovePublicationById

## Location
src/backend/commands/publicationcmds.c: 1482 - 1510

## Overview
RemovePublicationById removes a publication from the system catalog using its OID, performing appropriate cache invalidation based on the publication type.

## Definition


## Detailed Description
This function completely removes a publication from the pg_publication catalog table using the publication's OID. It implements intelligent cache invalidation logic that handles FOR ALL TABLES publications specially by invalidating the entire relation cache, while regular publications rely on dependency-driven invalidation. The function is primarily called by the dependency system during cascading deletions when a publication is explicitly dropped or when related objects are removed.

## Parameters / Member Variables
- : OID of the publication to be removed from the pg_publication catalog

## Dependencies
- Functions called/Symbols referenced:
  - table_open (catalog access)
  - SearchSysCache1 (cache lookup)
  - CacheInvalidateRelcacheAll (global cache invalidation)
  - CatalogTupleDelete (catalog modification)
  - ReleaseSysCache (cache cleanup)
- Called from (representative examples):
  - doDeletion (dependency system)

## Notes and Other Information
- Uses RowExclusiveLock on PublicationRelationId for safe concurrent operations
- FOR ALL TABLES publications trigger global relation cache invalidation via CacheInvalidateRelcacheAll()
- Regular publications rely on dependency-based cache invalidation for affected relations
- Part of PostgreSQL's dependency system for clean object removal
- Error handling includes cache lookup failure detection
- Essential for maintaining logical replication consistency during publication drops
- Simpler than RemovePublicationRelById as it handles entire publications rather than individual relations