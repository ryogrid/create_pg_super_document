# RemoveTriggerById

## Location
[src/backend/commands/trigger.c:1287-1365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L1287-L1365)

## Overview
RemoveTriggerById performs the core trigger deletion operation by removing the trigger tuple from pg_trigger and invalidating the relation cache to ensure consistency.

## Definition
```c
void RemoveTriggerById(Oid trigOid)
```

## Detailed Description
RemoveTriggerById is the fundamental function for deleting triggers in PostgreSQL. It locates the trigger by OID in the pg_trigger system catalog, validates that the owning relation is a valid trigger-supporting relation type, enforces system catalog protection rules, and removes the trigger tuple. The function takes an exclusive lock on the target relation to ensure consistency during the deletion process. After removing the catalog entry, it forces a relcache invalidation to notify all backends that the relation's trigger metadata has changed, ensuring that cached trigger information is rebuilt.

## Parameters / Member Variables
- `trigOid`: OID of the trigger to be deleted from the system catalog

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
  - [IsSystemRelation](../I/IsSystemRelation.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - Form_pg_trigger
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md)

## Notes and Other Information
- Takes AccessExclusiveLock on the trigger's relation to prevent concurrent modifications
- Validates relation types (RELATION, VIEW, FOREIGN_TABLE, PARTITIONED_TABLE) support triggers
- Protects system catalogs from unauthorized trigger deletion
- Forces relcache invalidation instead of trying to determine if other triggers remain
- Keeps lock on trigger's relation until end of transaction for consistency
- Does not update relhastriggers flag, relying on relcache rebuild to handle it correctly
- Part of PostgreSQL's dependency management system, typically called during CASCADE deletions
- Used internally by the dependency system when triggers are dropped as part of larger operations