# TypeCacheOpcCallback

## Location
[src/backend/utils/cache/typcache.c:2395-2423](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L2395-L2423)

## Overview
TypeCacheOpcCallback is a syscache invalidation callback function that handles updates to the pg_opclass system catalog by invalidating all cached operator class-related data in the type cache.

## Definition
static void TypeCacheOpcCallback(Datum arg, int cacheid, uint32 hashvalue)

## Detailed Description
This function serves as a callback that is invoked whenever a syscache invalidation event occurs for any row in the pg_opclass system catalog. Rather than attempting to selectively invalidate only data dependent on the specific opclass that changed, the function takes a simpler approach and marks all cached operator-related data as invalid. This design choice is justified because updates to pg_opclass are rare in production environments.

The function iterates through all entries in the TypeCacheHash and clears the operator-related validity flags (TCFLAGS_OPERATOR_FLAGS) from each type cache entry. This ensures that any cached equality, comparison, or hashing operator information will be recomputed on next access.

The function intentionally does not monitor updates to pg_amop or pg_amproc catalogs, as ALTER OPERATOR FAMILY operations are restricted from modifying primary operators and functions of an opclass - they can only affect cross-type family members that are not cached by the type cache system.

## Parameters / Member Variables
- : Datum argument passed by the syscache callback mechanism (unused in this function)
- : Cache identifier indicating which syscache triggered the invalidation (unused in this function)  
- : Hash value associated with the invalidated entry (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - HASH_SEQ_STATUS
  - TCFLAGS_OPERATOR_FLAGS
- Called from (representative examples):
  - [lookup_type_cache](../l/lookup_type_cache.md) (callback registration)

## Notes and Other Information
- This is a static function, only accessible within typcache.c
- The function assumes TypeCacheHash exists when called, as the callback wouldn't be registered otherwise
- The design prioritizes simplicity over optimization due to the rarity of pg_opclass updates
- Part of PostgreSQL's type cache invalidation mechanism for maintaining data consistency