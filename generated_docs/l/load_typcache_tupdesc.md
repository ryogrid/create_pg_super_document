# load_typcache_tupdesc

## Location
src/backend/utils/cache/typcache.c: 880 - 913

## Overview
A helper function that loads and caches the tuple descriptor for composite types by opening the associated relation and storing a reference to its descriptor.

## Definition


## Detailed Description
This function is responsible for populating the tupDesc field in a TypeCacheEntry for composite (row) types. It opens the relation corresponding to the composite type using the stored typrelid, extracts the relation's tuple descriptor, and stores a reference to it in the cache entry.

The function carefully manages reference counting for the tuple descriptor to ensure it remains valid even after the relation is closed. It manually increments the descriptor's reference count rather than using the standard IncrTupleDescRefCount() function because the reference must outlive the current query and shouldn't be tracked by the current resource owner.

Additionally, the function assigns a unique identifier to the cached tuple descriptor by incrementing a global counter. This identifier can be used to detect when the descriptor has changed in future cache lookups.

## Parameters / Member Variables
- : Pointer to the TypeCacheEntry structure to populate with tuple descriptor information

## Dependencies
- Functions called/Symbols referenced:
  - relation_open (to open the relation associated with the composite type)
  - relation_close (to close the relation after extracting the descriptor)
  - RelationGetDescr (macro to get the tuple descriptor from a relation)
- Called from (representative examples):
  - lookup_type_cache (when TYPECACHE_TUPDESC flag is requested)
  - cache_record_field_properties (when analyzing record field properties)

## Notes and Other Information
- This is a static helper function only used within typcache.c
- Performs error checking to ensure the composite type has a valid relation OID
- Uses manual reference count management rather than the standard resource owner system
- Assigns a unique identifier to detect descriptor changes in the future
- The function assumes the composite type's relation exists and is accessible
- Holds AccessShareLock on the relation during descriptor extraction