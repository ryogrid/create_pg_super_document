# SetRelationNumChecks

## Location
[src/backend/catalog/heap.c:2712-2745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L2712-L2745)

## Overview
SetRelationNumChecks updates the count of check constraints in a relation's pg_class tuple and ensures relcache invalidation across all backends.

## Definition


## Detailed Description
SetRelationNumChecks is a static function that updates the relchecks field in the pg_class catalog table for a given relation. The function serves a dual purpose: maintaining accurate constraint counts and triggering relcache invalidation across all PostgreSQL backends.

The function performs the following operations:
1. Opens the pg_class relation with RowExclusiveLock
2. Retrieves the current pg_class tuple for the target relation
3. Compares the current relchecks value with the new count
4. Updates the tuple if the count has changed, or forces relcache invalidation if unchanged
5. Ensures proper cleanup of allocated memory and relation locks

The relcache invalidation is crucial because it forces other backends to rebuild their cached relation information, ensuring they see the updated constraint information. This is particularly important for constraint checking and query planning.

## Parameters / Member Variables
- : The relation whose check constraint count is being updated
- : The new number of check constraints for the relation

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_class
  - SearchSysCacheCopy1
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [StoreConstraints](StoreConstraints.md)
  - [AddRelationNewConstraints](../A/AddRelationNewConstraints.md)

## Notes and Other Information
- Caller must hold exclusive lock on the relation to ensure consistency
- Forces relcache invalidation even when the count hasn't changed to ensure SI (Shared Invalidation) messages are sent
- The relcache entry for the current backend will be rebuilt at the next CommandCounterIncrement
- Always triggers either a catalog update or explicit cache invalidation to maintain system consistency
- Properly manages memory by freeing the copied heap tuple after use
- Uses system cache lookups for efficient access to pg_class tuples
- Critical for maintaining accurate metadata about relation constraints across the cluster