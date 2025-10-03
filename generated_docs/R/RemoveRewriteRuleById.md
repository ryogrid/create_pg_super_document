# RemoveRewriteRuleById

## Location
[src/backend/rewrite/rewriteRemove.c:33-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteRemove.c#L33-L94)

## Overview
Removes a rewrite rule from the PostgreSQL system catalogs by its OID, handling all necessary cleanup and cache invalidation.

## Definition

```c
void
RemoveRewriteRuleById(Oid ruleOid)
```
## Detailed Description
This function implements the core logic for deleting a rewrite rule from the PostgreSQL system. It performs several critical operations:

1. Opens the pg_rewrite system catalog with exclusive lock
2. Locates the target rule tuple using the provided OID
3. Acquires AccessExclusiveLock on the event relation to prevent concurrent queries that might depend on the rule
4. Validates permissions for system catalog modifications
5. Deletes the rule tuple from pg_rewrite
6. Issues cache invalidation to update all backends with the new rule set

The function ensures data consistency by using appropriate locking mechanisms and handles both user-defined and system rules with proper permission checks.

## Parameters / Member Variables
- `ruleOid`: The object identifier (OID) of the rewrite rule to be removed from the system catalogs
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (to access pg_rewrite and event relations)
  - [ScanKeyInit](../S/ScanKeyInit.md) (to initialize scan key for rule lookup)
  - [systable_beginscan](../s/systable_beginscan.md) (to begin system table scan)
  - [systable_getnext](../s/systable_getnext.md) (to retrieve rule tuple)
  - [systable_endscan](../s/systable_endscan.md) (to end system table scan)
  - [IsSystemRelation](../I/IsSystemRelation.md) (to check if target relation is a system catalog)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (to delete the rule tuple)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md) (to invalidate relation cache)
  - [table_close](../t/table_close.md) (to close opened relations)
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md) (from dependency.c:1412 - part of dependency deletion cascade)

## Notes and Other Information
- The function acquires AccessExclusiveLock on the event relation to ensure no concurrent queries depend on the rule being deleted
- System catalog modifications are protected by the allowSystemTableMods flag
- Cache invalidation is essential to notify all backends about the rule removal
- The event relation lock is held until transaction commit to maintain consistency
- Error handling includes validation that the rule tuple exists before attempting deletion
- The function is declared in src/include/rewrite/rewriteRemove.h