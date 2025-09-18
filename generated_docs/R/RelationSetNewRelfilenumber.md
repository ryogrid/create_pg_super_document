# RelationSetNewRelfilenumber

## Location
[src/backend/utils/cache/relcache.c:3769-3970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L3769-L3970)

## Overview
RelationSetNewRelfilenumber assigns a new physical file number (and optionally new persistence setting) to a relation, enabling transactionally safe full rewrites of relations.

## Definition


## Detailed Description
This function performs a complete relfilenumber change operation for a relation, which effectively creates new physical storage while maintaining transactional safety. The process involves several coordinated steps:

1. **File Number Allocation**: Either allocates a new relfilenumber via GetNewRelFileNumber() or uses pre-assigned numbers during binary upgrades
2. **Catalog Updates**: Updates the pg_class catalog entry with the new relfilenumber and related statistics
3. **Storage Management**: Creates new physical storage and schedules the old storage for deletion at transaction commit
4. **Mapping Handling**: For mapped relations (system catalogs), updates the relation mapping instead of pg_class.relfilenode
5. **Statistics Reset**: Resets relation statistics (relpages, reltuples, relallvisible) since the new storage starts empty
6. **Transaction Integration**: Ensures proper XID assignment for file deletion operations and cache invalidation

The function handles different relation types appropriately, using table access methods for table-like objects and direct storage creation for others. For binary upgrades, it immediately removes old storage rather than deferring to commit time.

Special handling exists for mapped relations where pg_class.relfilenode doesn't change, and updates go through the relation mapper instead. This is essential when reindexing system catalogs like pg_class itself.

## Parameters / Member Variables
- : The relation to assign a new relfilenumber to
- : New persistence setting (permanent, temporary, or unlogged)

## Dependencies
- Functions called/Symbols referenced:
  - [GetNewRelFileNumber](../G/GetNewRelFileNumber.md)
  - [SearchSysCacheLockedCopy1](../S/SearchSysCacheLockedCopy1.md)
  - [RelationDropStorage](RelationDropStorage.md)
  - [RelationCreateStorage](RelationCreateStorage.md)
  - table_relation_set_new_filelocator
  - [RelationMapUpdateMap](RelationMapUpdateMap.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - CommandCounterIncrement
  - [RelationAssumeNewRelfilelocator](RelationAssumeNewRelfilelocator.md)
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [reindex_index](../r/reindex_index.md)
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)
  - [ResetSequence](ResetSequence.md)
  - [AlterSequence](../A/AlterSequence.md)
  - [SequenceChangePersistence](../S/SequenceChangePersistence.md)

## Notes and Other Information
- Caller must hold exclusive lock on the relation before calling this function
- The operation limits access to the relation's old data for the remainder of the current transaction
- Binary upgrade mode uses pre-assigned relfilenumbers instead of allocating new ones
- For mapped relations, pg_class statistics may become temporarily inaccurate but will be corrected later
- Sequences preserve their relpages/reltuples statistics since they don't change during the operation
- The function ensures transactional safety by scheduling old storage deletion for commit time (except during binary upgrades)
- Table access methods handle creation of both main fork and initialization fork as needed
- The operation triggers cache invalidation to ensure the relcache reflects the new relfilenumber