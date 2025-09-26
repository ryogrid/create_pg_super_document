# logicalrep_partmap_init

## Location
[src/backend/replication/logical/relation.c:567-601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/relation.c#L567-L601)

## Overview
Initializes the logical replication partition map cache system, setting up the hash table and memory context for tracking partition mappings.

## Definition
```c
static void logicalrep_partmap_init(void)
```

## Detailed Description
This function initializes the logical replication partition map cache infrastructure. It creates a dedicated memory context for partition map operations and sets up a hash table to cache partition mappings between publisher and subscriber. The function also registers a callback for relation cache invalidation events to ensure the partition map stays synchronized with relation changes.

The hash table is configured to use partition OIDs as keys and stores LogicalRepPartMapEntry structures. The function uses PostgreSQL's standard hash table implementation with blob keys and a specific memory context for efficient memory management.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [hash_create](../h/hash_create.md)
  - [CacheRegisterRelcacheCallback](../C/CacheRegisterRelcacheCallback.md)
  - [logicalrep_partmap_invalidate_cb](logicalrep_partmap_invalidate_cb.md)
- Types referenced:
  - [HASHCTL](../H/HASHCTL.md)
  - [LogicalRepPartMapEntry](../L/LogicalRepPartMapEntry.md)
- Global variables:
  - LogicalRepPartMapContext
  - LogicalRepPartMap  
  - CacheMemoryContext
- [Hash](../H/Hash.md) flags used:
  - HASH_ELEM
  - HASH_BLOBS
  - HASH_CONTEXT
- Called from (representative examples):
  - [logicalrep_partition_open](logicalrep_partition_open.md)

## Notes and Other Information
- This is a static function, only accessible within the relation.c file
- Creates the memory context only if it doesn't already exist, allowing safe multiple calls
- The hash table is sized with an initial capacity of 64 entries
- Registers an invalidation callback to handle relation cache changes that might affect partition mappings
- Part of the logical replication subsystem's caching mechanism for efficient partition handling
- The memory context name "LogicalRepPartMapContext" helps with debugging and memory analysis