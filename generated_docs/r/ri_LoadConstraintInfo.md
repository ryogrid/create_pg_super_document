# ri_LoadConstraintInfo

## Location
src/backend/utils/adt/ri_triggers.c: 2112 - 2193

## Overview
Fetches or creates the RI_ConstraintInfo struct for a foreign key constraint by loading constraint data from the system catalog and caching it in a hash table.

## Definition


## Detailed Description
This function implements a caching mechanism for foreign key constraint information. It first checks if the constraint info is already cached in ri_constraint_cache hash table. If not found or invalid, it:

1. Fetches the constraint row from pg_constraint system catalog
2. Validates that it's a foreign key constraint
3. Extracts constraint metadata (names, relation OIDs, action types, etc.)
4. Uses DeconstructFkConstraintRow to parse column mappings and operators
5. Adds the entry to a valid constraint list for efficient invalidation
6. Marks the entry as valid and returns it

The function handles constraint inheritance by determining the root constraint ID for partitioned foreign keys.

## Parameters / Member Variables
- : OID of the foreign key constraint to load

## Dependencies
- Functions called/Symbols referenced:
  - ri_InitHashTables
  - hash_search
  - SearchSysCache1
  - HeapTupleIsValid
  - elog
  - get_ri_constraint_root
  - GetSysCacheHashValue1
  - DeconstructFkConstraintRow
  - ReleaseSysCache
  - dclist_push_tail
- Called from (representative examples):
  - ri_FetchConstraintInfo

## Notes and Other Information
- Uses a hash table (ri_constraint_cache) for efficient constraint info caching
- Initializes the hash table on first call via ri_InitHashTables
- Maintains a doubly-linked list (ri_constraint_cache_valid_list) of valid entries for invalidation processing
- Stores both constraint OID and root constraint OID hash values for efficient lookups
- Handles partitioned foreign key constraints by tracking constraint hierarchy
- Returns a const pointer to prevent modification of cached data
- Located in src/backend/utils/adt/ri_triggers.c:2112-2193