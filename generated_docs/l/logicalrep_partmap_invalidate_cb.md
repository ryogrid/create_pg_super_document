# logicalrep_partmap_invalidate_cb

## Location
src/backend/replication/logical/relation.c: 492 - 539

## Overview
A relation cache invalidation callback function that invalidates partition map entries when the underlying relations are modified or dropped, ensuring consistency between the logical replication partition cache and the actual database state.

## Definition
```c
static void logicalrep_partmap_invalidate_cb(Datum arg, Oid reloid)
```

## Detailed Description
This function serves as a callback that is invoked by PostgreSQL's relation cache invalidation system when relations are modified, dropped, or when a global invalidation occurs. It maintains the consistency of the logical replication partition map (LogicalRepPartMap) by marking affected entries as invalid when their underlying relations change.

The function handles two scenarios: specific relation invalidation (when reloid is valid) and global invalidation (when reloid is InvalidOid). For specific invalidations, it searches through the partition map to find entries matching the invalidated relation OID and marks them as invalid. For global invalidations, it marks all entries in the partition map as invalid.

The partition map is distinct from the regular relation map because it is keyed by partition OID rather than remote relation OID, specifically to handle cases where partitions are replicated through their ancestor relations rather than being directly mapped to remote relations.

## Parameters / Member Variables
- `arg`: Datum argument passed by the callback system (unused in this function)
- `reloid`: OID of the relation being invalidated, or InvalidOid for global invalidation

## Dependencies
- Functions called/Symbols referenced:
  - hash_seq_init: Initializes hash table sequential scanning
  - hash_seq_search: Gets the next entry during sequential hash table scanning
  - hash_seq_term: Terminates hash table sequential scanning early
  - LogicalRepPartMapEntry: Structure type for partition map entries
  - HASH_SEQ_STATUS: Structure type for tracking hash table sequential scan state
- Called from (representative examples):
  - logicalrep_partmap_init: Registers this callback with the relation cache invalidation system

## Notes and Other Information
- This is a static function, only accessible within the relation.c file
- The function includes a TODO comment suggesting the use of an inverse lookup hashtable for better performance
- Currently uses linear search through the hash table, which could be optimized for tables with many partitions
- The callback is registered with PostgreSQL's relcache invalidation system during partition map initialization
- Essential for maintaining consistency in logical replication when DDL operations affect partitioned tables
- The function safely handles the case where LogicalRepPartMap is NULL (not initialized)
- Global invalidation (InvalidOid) marks all partition entries as invalid, forcing them to be rebuilt on next access
- Part of the broader logical replication cache management system that ensures data consistency across DDL operations