# TypeCacheTypCallback

## Location
src/backend/utils/cache/typcache.c: 2355 - 2394

## Overview
Syscache invalidation callback function that marks cached type information as invalid when pg_type rows are modified, ensuring type cache consistency with system catalog changes.

## Definition


## Detailed Description
This function serves as an invalidation callback for the system cache (syscache) that responds to changes in the pg_type system catalog. When type definitions are modified in the database (such as through ALTER TYPE, CREATE TYPE, DROP TYPE commands), this callback ensures that any cached information about those types is properly invalidated.

The function scans through all entries in the TypeCacheHash and identifies entries that correspond to the modified type using hash value comparison. For matching entries (or all entries during a total cache flush when hashvalue is 0), it clears flags that indicate the validity of cached pg_type data.

Specifically, it invalidates TCFLAGS_HAVE_PG_TYPE_DATA to force reloading of basic type information from pg_type, and TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS for domain types since domain constraints might have changed (particularly the typnotnull property).

## Parameters / Member Variables
- : Callback argument (unused in this implementation)
- : System cache ID that triggered the invalidation
- : Hash value identifying the specific type row that changed, or 0 for total cache flush

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - TCFLAGS_HAVE_PG_TYPE_DATA
  - TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS
- Called from (representative examples):
  - [lookup_type_cache](../l/lookup_type_cache.md) (registers the callback)

## Notes and Other Information
- Registered as a syscache invalidation callback during type cache initialization
- Uses hash value comparison for efficient identification of affected cache entries
- Handles both specific type invalidations and total cache flushes (hashvalue == 0)
- Critical for maintaining consistency between cached type information and system catalog changes
- Particularly important for domain types where constraint information might change
- More efficient than relation-based invalidation since it can target specific types
- Ensures that subsequent type cache lookups will refresh data from pg_type catalog