# RelfilenumberMapInvalidateCallback

## Location
src/backend/utils/cache/relfilenumbermap.c: 52 - 85

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
  - hash_seq_init
  - hash_seq_search  
  - hash_search
  - HASH_REMOVE
  - HASH_SEQ_STATUS
  - RelfilenumberMapEntry
- Called from (representative examples):
  - InitializeRelfilenumberMap (registered as callback)

## Notes and Other Information
- This is a static function only used within the relfilenumbermap.c module
- The function assumes RelfilenumberMapHash is already initialized (enforced by Assert)
- Uses hash table sequential scanning to examine all entries efficiently
- Critical for maintaining cache consistency when pg_class catalog changes occur
- Registered as a system cache invalidation callback during initialization