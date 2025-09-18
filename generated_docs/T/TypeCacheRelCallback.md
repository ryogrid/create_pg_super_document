# TypeCacheRelCallback

## Location
src/backend/utils/cache/typcache.c: 2290 - 2354

## Overview
Relcache invalidation callback function that cleans up cached tuple descriptors and resets comparability information for composite types when their underlying relations change.

## Definition


## Detailed Description
This function serves as an invalidation callback that responds to relcache invalidation events. When a relation is modified (such as through ALTER TABLE commands), this callback ensures that any cached information about composite types based on that relation is properly invalidated and cleaned up.

The function scans through all entries in the TypeCacheHash to find composite types that correspond to the invalidated relation. For matching entries, it deallocates cached tuple descriptors using proper reference counting, clears the tupDesc_identifier to signal potential changes to observers, and resets operator-related flags (equality, comparison, hashing validity).

The function also handles domain types over composite types by resetting their operator flags when the underlying composite type might have changed. This ensures that cached comparison and hashing operations are recalculated after structural changes to composite types.

## Parameters / Member Variables
- : Callback argument (unused in this implementation)
- : OID of the relation being invalidated, or InvalidOid to invalidate all composite types

## Dependencies
- Functions called/Symbols referenced:
  - hash_seq_init
  - hash_seq_search
  - FreeTupleDesc
  - TYPTYPE_COMPOSITE
  - TYPTYPE_DOMAIN
  - TCFLAGS_OPERATOR_FLAGS
  - TCFLAGS_DOMAIN_BASE_IS_COMPOSITE
- Called from (representative examples):
  - lookup_type_cache (registers the callback)

## Notes and Other Information
- Registered as a relcache invalidation callback during type cache initialization
- Scans entire TypeCacheHash since type OID to relation mapping isn't directly available
- Uses proper reference counting when deallocating tuple descriptors
- Cannot use DecrTupleDescRefCount because the reference isn't logged in current resource owner
- Clears tupDesc_identifier to notify observers of potential descriptor changes
- Handles both direct composite types and domain types over composite types
- Design trades callback registration complexity for runtime scanning cost
- Alternative designs (second hashtable, syscache callbacks) were considered but deemed not worthwhile