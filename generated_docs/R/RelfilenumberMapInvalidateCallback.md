# RelfilenumberMapInvalidateCallback

## Location
[src/backend/utils/cache/relfilenumbermap.c:52-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relfilenumbermap.c#L52-L85)

## Overview
RelfilenumberMapInvalidateCallback is a static callback function that flushes mapping entries from the relfilenumber-to-relid cache when pg_class is updated in a relevant fashion.

## Definition
```c
static void RelfilenumberMapInvalidateCallback(Datum arg, Oid relid)
```

## Detailed Description
This function serves as a cache invalidation callback that maintains consistency of the relfilenumber mapping hash table when the pg_class system catalog is modified. It iterates through all entries in the RelfilenumberMapHash and removes entries based on specific conditions:

- If relid is InvalidOid (signaling a complete reset), all entries are removed
- If an entry has relid as InvalidOid (negative cache entry), it is removed
- If an entry matches the specific relid being invalidated, it is removed

The function ensures that stale cache entries are cleaned up whenever relevant changes occur to relation metadata, preventing inconsistencies between the cached mappings and the actual system catalog state.

## Parameters / Member Variables
- `arg`: Datum argument (unused in this implementation)
- `relid`: The relation OID that triggered the invalidation, or InvalidOid for complete cache reset

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)  
  - [hash_search](../h/hash_search.md)
  - HASH_REMOVE
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - RelfilenumberMapEntry
- Called from (representative examples):
  - [InitializeRelfilenumberMap](../I/InitializeRelfilenumberMap.md) (registered as callback)

## Notes and Other Information
- This is a static function only used within the relfilenumbermap.c module
- The function assumes RelfilenumberMapHash is already initialized (enforced by Assert)
- Uses hash table sequential scanning to examine all entries efficiently
- Critical for maintaining cache consistency when pg_class catalog changes occur
- Registered as a system cache invalidation callback during initialization

## Simplified Source

```c
static void
RelfilenumberMapInvalidateCallback(Datum arg, Oid relid)
{
    HASH_SEQ_STATUS status;
    RelfilenumberMapEntry *entry;

    Assert(RelfilenumberMapHash != NULL);

    // Iterate through all entries in the hash table
    hash_seq_init(&status, RelfilenumberMapHash);
    while ((entry = (RelfilenumberMapEntry *) hash_seq_search(&status)) != NULL) {
        // Remove entries based on invalidation criteria:
        // - Complete reset (relid == InvalidOid)
        // - Negative cache entries (entry->relid == InvalidOid)
        // - Specific relation being invalidated (entry->relid == relid)
        if (relid == InvalidOid ||
            entry->relid == InvalidOid ||
            entry->relid == relid) {

            if (hash_search(RelfilenumberMapHash, &entry->key, HASH_REMOVE, NULL) == NULL)
                elog(ERROR, "hash table corrupted");
        }
    }
}
```