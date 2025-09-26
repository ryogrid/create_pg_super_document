# logicalrep_relmap_invalidate_cb

## Location
[src/backend/replication/logical/relation.c:64-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/relation.c#L64-L104)

## Overview
A relcache invalidation callback function that invalidates logical replication relation map cache entries when the underlying relations change.

## Definition
static void logicalrep_relmap_invalidate_cb(Datum arg, Oid reloid)

## Detailed Description
This function serves as a callback for PostgreSQL's relcache invalidation system, specifically for maintaining the logical replication relation map cache. When a relation is modified or dropped, this callback ensures that the corresponding cached entries in the logical replication relation map are marked as invalid to prevent stale data from being used.

The function operates in two modes:
1. **Specific relation invalidation**: When a specific relation OID is provided, it searches through the hash table to find entries matching that relation and marks them as invalid
2. **Global invalidation**: When InvalidOid is passed, it invalidates all entries in the cache

The implementation uses a sequential scan through the hash table, which includes a TODO comment suggesting that an inverse lookup hashtable could improve performance for specific relation invalidations.

## Parameters / Member Variables
- : Datum argument (unused in this implementation, typically used for callback context)
- : The OID of the relation to invalidate, or InvalidOid to invalidate all entries

## Dependencies
- Functions called/Symbols referenced:
  - hash_seq_init
  - hash_seq_search
  - hash_seq_term
  - LogicalRepRelMapEntry (struct type)
  - HASH_SEQ_STATUS (struct type)
- Called from (representative examples):
  - logicalrep_relmap_init (registered as callback)

## Notes and Other Information
- This is a static function, only accessible within the relation.c file
- Contains a TODO comment suggesting performance optimization with inverse lookup hashtable
- Uses PostgreSQL's hash table sequential scanning mechanism
- The function includes a safety check to ensure LogicalRepRelMap is not NULL
- Part of the logical replication subsystem's cache management infrastructure