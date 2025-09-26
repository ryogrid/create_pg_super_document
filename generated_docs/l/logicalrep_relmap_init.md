# logicalrep_relmap_init

## Location
[src/backend/replication/logical/relation.c:105-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/relation.c#L105-L131)

## Overview
Initializes the logical replication relation map cache system, creating the hash table and memory context for caching relation mappings.

## Definition
static void logicalrep_relmap_init(void)

## Detailed Description
This function performs the one-time initialization of the logical replication relation map cache infrastructure. It sets up both the memory management context and the hash table used to cache relation mappings during logical replication operations.

The function creates a dedicated memory context under CacheMemoryContext to manage all allocations related to the relation map cache. It then initializes a hash table with appropriate configuration for storing LogicalRepRelMapEntry objects, keyed by LogicalRepRelId structures.

After setting up the core data structures, the function registers a relcache invalidation callback to ensure cache consistency when underlying relations are modified or dropped.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [hash_create](../h/hash_create.md)
  - [CacheRegisterRelcacheCallback](../C/CacheRegisterRelcacheCallback.md)
  - [logicalrep_relmap_invalidate_cb](logicalrep_relmap_invalidate_cb.md)
  - LogicalRepRelId (key structure type)
  - [LogicalRepRelMapEntry](../L/LogicalRepRelMapEntry.md) (entry structure type)
  - [HASHCTL](../H/HASHCTL.md) (configuration structure)
  - ALLOCSET_DEFAULT_SIZES
  - HASH_ELEM, HASH_BLOBS, HASH_CONTEXT (hash table flags)
- Called from (representative examples):
  - [logicalrep_relmap_update](logicalrep_relmap_update.md)
  - [logicalrep_rel_open](logicalrep_rel_open.md)

## Notes and Other Information
- This is a static function, only accessible within the relation.c file
- Creates a memory context specifically for logical replication relation mappings
- Uses a hash table with initial capacity of 128 entries
- [Hash](../H/Hash.md) table is configured with HASH_ELEM | HASH_BLOBS | HASH_CONTEXT flags for optimal performance
- Registers an invalidation callback to maintain cache consistency
- Lazy initialization pattern - only creates context if it doesn't already exist
- Part of PostgreSQL's logical replication subsystem cache management