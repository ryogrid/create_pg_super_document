# IsSharedRelation

## Location
src/backend/catalog/catalog.c: 273 - 342

## Overview
IsSharedRelation determines whether a given relation (identified by OID) is supposed to be shared across an entire PostgreSQL database cluster.

## Definition


## Detailed Description
This function checks if a relation is shared across the entire database cluster rather than being specific to a single database. Shared relations include system catalogs that contain cluster-wide information (like roles, databases, tablespaces) and their associated indexes and TOAST tables. The function uses a hard-coded list of OIDs for performance and locking reasons. While MVCC catalog access has reduced some race conditions that originally required this approach, the current implementation avoids potential complexity and performance issues that could arise from dynamically checking the pg_class.relisshared field.

## Parameters / Member Variables
- : The OID of the relation to check

## Dependencies
- Functions called/Symbols referenced: None (uses only OID constant comparisons)
- Called from (representative examples):
  - SetLocktagRelationOid (src/backend/storage/lmgr/lmgr.c:93)
  - CacheInvalidateHeapTuple (src/backend/utils/cache/inval.c:1246)
  - CacheInvalidateCatalog (src/backend/utils/cache/inval.c:1345)
  - RelationBuildLocalRelation (src/backend/utils/cache/relcache.c:3575)
  - get_object_address (src/backend/catalog/objectaddress.c:1157, 1174)
  - check_lock_if_inplace_updateable_rel (src/backend/access/heap/heapam.c:4218)
  - pgstat_fetch_stat_tabentry (src/backend/utils/activity/pgstat_relation.c:458)
  - GetSecurityLabel (src/backend/commands/seclabel.c:283)
  - SetSecurityLabel (src/backend/commands/seclabel.c:417)

## Notes and Other Information
- The function checks three categories of shared relations: shared catalogs (marked with BKI_SHARED_RELATION), their indexes, and their TOAST tables and indexes
- Shared catalogs include: pg_authid, pg_auth_members, pg_database, pg_db_role_setting, pg_parameter_acl, pg_replication_origin, pg_shdepend, pg_shdescription, pg_shseclabel, pg_subscription, pg_tablespace
- This is a performance-critical function used in locking and cache invalidation operations
- The hard-coded approach avoids the complexity of scanning pg_class during critical locking operations