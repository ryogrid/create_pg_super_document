# lookup_rowtype_tupdesc_internal

## Location
src/backend/utils/cache/typcache.c: 1739 - 1832

## Overview
Internal routine to lookup a row type's tuple descriptor, handling both named composite types and transient record types, with support for shared typmod registries in parallel processing environments.

## Definition


## Detailed Description
This static function serves as the core implementation for row type tuple descriptor lookup in PostgreSQL's type cache system. It handles two distinct categories of row types:

1. **Named composite types** (non-RECORDOID): Uses the regular type cache system via lookup_type_cache() to retrieve the tuple descriptor for registered composite types.

2. **Transient record types** (RECORDOID): Manages dynamically created record types through a more complex caching mechanism that supports:
   - Local cache lookup in RecordCacheArray
   - Shared typmod registry lookup for parallel processing scenarios
   - Dynamic shared memory integration via dsa_get_address()

For transient record types, the function first checks the local cache, then queries the shared typmod registry if available. When found in shared memory, it establishes a local cache entry pointing to the shared tuple descriptor. The returned tuple descriptor has no reference count bumping, distinguishing this internal function from its public counterparts.

## Parameters / Member Variables
- : The OID of the type being looked up (RECORDOID for transient records, other OIDs for named composite types)
- : Type modifier identifying the specific record type variant (used for transient records)
- : If true, returns NULL instead of throwing an error when the type is not found

## Dependencies
- Functions called/Symbols referenced:
  - lookup_type_cache
  - dshash_find
  - dsa_get_address
  - ensure_record_cache_typmod_slot_exists
  - dshash_release_lock
  - TYPECACHE_TUPDESC
  - SharedTypmodTableEntry
- Called from (representative examples):
  - lookup_rowtype_tupdesc
  - lookup_rowtype_tupdesc_noerror
  - lookup_rowtype_tupdesc_copy
  - lookup_rowtype_tupdesc_domain

## Notes and Other Information
- This is a static function internal to typcache.c, not exposed to external modules
- The returned tuple descriptor does not have its reference count incremented, unlike public APIs
- Supports PostgreSQL's parallel processing architecture through shared typmod registries
- Handles both error-throwing and no-error variants through the noError parameter
- For shared tuple descriptors, tdrefcount is set to -1 to indicate non-reference-counted storage
- Local tupdesc identifiers are assigned uniquely per process, not shared across processes