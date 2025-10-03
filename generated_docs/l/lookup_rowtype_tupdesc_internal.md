# lookup_rowtype_tupdesc_internal

## Location
[src/backend/utils/cache/typcache.c:1739-1832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1739-L1832)

## Overview
Internal routine to lookup a row type's tuple descriptor, handling both named composite types and transient record types, with support for shared typmod registries in parallel processing environments.

## Definition

```c
static TupleDesc
lookup_rowtype_tupdesc_internal(Oid type_id, int32 typmod, bool noError)
```
## Detailed Description
This static function serves as the core implementation for row type tuple descriptor lookup in PostgreSQL's type cache system. It handles two distinct categories of row types:

1. **Named composite types** (non-RECORDOID): Uses the regular type cache system via lookup_type_cache() to retrieve the tuple descriptor for registered composite types.

2. **Transient record types** (RECORDOID): Manages dynamically created record types through a more complex caching mechanism that supports:
   - Local cache lookup in RecordCacheArray
   - Shared typmod registry lookup for parallel processing scenarios
   - Dynamic shared memory integration via dsa_get_address()

For transient record types, the function first checks the local cache, then queries the shared typmod registry if available. When found in shared memory, it establishes a local cache entry pointing to the shared tuple descriptor. The returned tuple descriptor has no reference count bumping, distinguishing this internal function from its public counterparts.

## Parameters / Member Variables
- `type_id`: The OID of the type being looked up (RECORDOID for transient records, other OIDs for named composite types)
- `typmod`: Type modifier identifying the specific record type variant (used for transient records)
- `noError`: If true, returns NULL instead of throwing an error when the type is not found
## Dependencies
- Functions called/Symbols referenced:
  - [lookup_type_cache](lookup_type_cache.md)
  - [dshash_find](../d/dshash_find.md)
  - [dsa_get_address](../d/dsa_get_address.md)
  - [ensure_record_cache_typmod_slot_exists](../e/ensure_record_cache_typmod_slot_exists.md)
  - [dshash_release_lock](../d/dshash_release_lock.md)
  - TYPECACHE_TUPDESC
  - [SharedTypmodTableEntry](../S/SharedTypmodTableEntry.md)
- Called from (representative examples):
  - [lookup_rowtype_tupdesc](lookup_rowtype_tupdesc.md)
  - [lookup_rowtype_tupdesc_noerror](lookup_rowtype_tupdesc_noerror.md)
  - [lookup_rowtype_tupdesc_copy](lookup_rowtype_tupdesc_copy.md)
  - [lookup_rowtype_tupdesc_domain](lookup_rowtype_tupdesc_domain.md)

## Notes and Other Information
- This is a static function internal to typcache.c, not exposed to external modules
- The returned tuple descriptor does not have its reference count incremented, unlike public APIs
- Supports PostgreSQL's parallel processing architecture through shared typmod registries
- Handles both error-throwing and no-error variants through the noError parameter
- For shared tuple descriptors, tdrefcount is set to -1 to indicate non-reference-counted storage
- Local tupdesc identifiers are assigned uniquely per process, not shared across processes

## Simplified Source
```c
static TupleDesc
lookup_rowtype_tupdesc_internal(Oid type_id, int32 typmod, bool noError)
{
    if (type_id != RECORDOID) {
        // Named composite type - use regular type cache
        TypeCacheEntry *typentry = lookup_type_cache(type_id, TYPECACHE_TUPDESC);

        if (typentry->tupDesc == NULL && !noError)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                           errmsg("type %s is not composite", format_type_be(type_id))));

        return typentry->tupDesc;
    }
    else {
        // Transient record type - check local cache first
        if (typmod >= 0) {
            // Check local cache
            if (typmod < RecordCacheArrayLen &&
                RecordCacheArray[typmod].tupdesc != NULL)
                return RecordCacheArray[typmod].tupdesc;

            // Check shared typmod registry if available
            if (CurrentSession->shared_typmod_registry != NULL) {
                SharedTypmodTableEntry *entry = dshash_find(CurrentSession->shared_typmod_table,
                                                           &typmod, false);
                if (entry != NULL) {
                    // Get tuple descriptor from shared memory
                    TupleDesc tupdesc = (TupleDesc) dsa_get_address(CurrentSession->area,
                                                                   entry->shared_tupdesc);

                    // Set up local cache entry
                    ensure_record_cache_typmod_slot_exists(typmod);
                    RecordCacheArray[typmod].tupdesc = tupdesc;
                    RecordCacheArray[typmod].id = ++tupledesc_id_counter;

                    dshash_release_lock(CurrentSession->shared_typmod_table, entry);
                    return tupdesc;
                }
            }
        }

        // Not found
        if (!noError)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                           errmsg("record type has not been registered")));
        return NULL;
    }
}
```