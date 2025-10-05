# rel_sync_cache_relation_cb

## Location
[src/backend/replication/pgoutput/pgoutput.c:2329-2379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L2329-L2379)

## Overview
A relcache invalidation callback function that marks relation sync cache entries as invalid when PostgreSQL's relation cache is invalidated.

## Definition

```c
structure here.  The next get_rel_sync_entry() call will
	 * rebuild it all.
	 */
	if (OidIsValid(relid))
	{
		/*
		 * Getting invalidations for relations that aren't in the table is
		 * entirely normal.  So we don't care if it's found or not.
		 */
		entry = (RelationSyncEntry *) hash_search(RelationSyncCache, &relid,
												  HASH_FIND, NULL);
		if (entry != NULL)
			entry->replicate_valid = false;
	}
	else
	{
		/* Whole cache must be flushed. */
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, RelationSyncCache);
		while ((entry = (RelationSyncEntry *) hash_seq_search(&status)) != NULL)
		{
			entry->replicate_valid = false;
		}
	}
}

/*
 * Publication relation/schema map syscache invalidation callback
 *
 * Called for invalidations on pg_publication, pg_publication_rel,
 * pg_publication_namespace, and pg_namespace.
 */
static void
rel_sync_cache_publication_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
```
## Detailed Description
This function serves as a callback registered with PostgreSQL's relation cache invalidation system. When the relation cache detects changes to relation definitions (such as schema modifications, permission changes, or relation drops), this callback is invoked to maintain consistency in the logical replication relation sync cache.

The function handles two scenarios:
1. **Specific Relation Invalidation**: When a specific relation OID is provided, it locates the corresponding entry in RelationSyncCache and marks it as invalid
2. **Global Cache Invalidation**: When relid is InvalidOid, it iterates through all entries in the cache and marks them all as invalid

The function uses a conservative approach - it only marks entries as invalid rather than deleting or modifying their substructures, since invalidation events can occur during active callback execution. The actual cleanup and rebuilding happens on the next get_rel_sync_entry() call.

## Parameters / Member Variables
- `NULL)`: Datum argument (unused in this implementation, required by callback signature)
- `false`: The OID of the relation being invalidated, or InvalidOid for global invalidation

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md) (lookup specific cache entry)
  - [hash_seq_init](../h/hash_seq_init.md) (initialize hash table iteration)  
  - [hash_seq_search](../h/hash_seq_search.md) (iterate through all hash entries)
  - OidIsValid (check if OID is valid)
- Called from (representative examples):
  - [init_rel_sync_cache](../i/init_rel_sync_cache.md) (registered as invalidation callback)

## Notes and Other Information
- Registered as a relcache invalidation callback during cache initialization
- Uses defensive programming - checks if RelationSyncCache exists before proceeding
- Does not free memory or modify substructures to avoid issues during concurrent access
- The replicate_valid flag being set to false triggers cache entry rebuilding on next access
- Handles both specific relation invalidation and global cache flushes
- Can receive invalidations for relations not in the cache, which is considered normal behavior
- Invalidation events can occur during syscache access within other logical decoding callbacks

## Simplified Source

```c
static void
rel_sync_cache_relation_cb(Datum arg, Oid relid)
{
    RelationSyncEntry *entry;

    // Early exit if cache doesn't exist
    if (RelationSyncCache == NULL)
        return;

    if (OidIsValid(relid))
    {
        // Invalidate specific relation entry
        entry = hash_search(RelationSyncCache, &relid, HASH_FIND, NULL);
        if (entry != NULL)
            entry->replicate_valid = false;
    }
    else
    {
        // Invalidate entire cache
        HASH_SEQ_STATUS status;
        hash_seq_init(&status, RelationSyncCache);
        while ((entry = hash_seq_search(&status)) != NULL)
        {
            entry->replicate_valid = false;
        }
    }
}
```