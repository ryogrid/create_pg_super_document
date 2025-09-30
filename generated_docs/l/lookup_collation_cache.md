# lookup_collation_cache

## Location
[src/backend/utils/adt/pg_locale.c:1253-1339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L1253-L1339)

## Overview
lookup_collation_cache is a static function that manages a hash table cache for collation information, storing flags about whether a collation's LC_COLLATE or LC_CTYPE is C/POSIX for performance optimization.

## Definition
```c
static collation_cache_entry *lookup_collation_cache(Oid collation, bool set_flags)
```

## Detailed Description
This function implements a caching mechanism for collation properties to avoid repeated catalog lookups. It maintains a hash table that stores whether each collation's LC_COLLATE and LC_CTYPE settings are equivalent to "C" or "POSIX". The cache is populated lazily - entries are created when first accessed and flags are set only when requested via the set_flags parameter. The function handles different collation providers (BUILTIN and LIBC) with appropriate logic for determining C/POSIX equivalence. The cache persists for the lifetime of a backend process and cannot be flushed, which is acceptable since PostgreSQL doesn't support ALTER COLLATION.

## Parameters / Member Variables
- `collation`: OID of the collation to look up in the cache
- `set_flags`: Boolean indicating whether to populate the collate_is_c and ctype_is_c flags if not already set

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md)
  - [hash_search](../h/hash_search.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - TextDatumGetCString
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [lc_collate_is_c](lc_collate_is_c.md)
  - [lc_ctype_is_c](lc_ctype_is_c.md)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md)

## Notes and Other Information
- The cache is initialized on first use with a hash table capacity of 100 entries
- Cache entries are never invalidated, which is safe since collations cannot be altered
- The function asserts that collation is valid and not DEFAULT_COLLATION_OID
- Different handling for COLLPROVIDER_BUILTIN vs COLLPROVIDER_LIBC collations
- Critical for performance in code paths that frequently check if locale is C/POSIX

## Simplified Source
```c
static collation_cache_entry *
lookup_collation_cache(Oid collation, bool set_flags)
{
    collation_cache_entry *cache_entry;
    bool found;

    // Initialize hash table on first use
    if (collation_cache == NULL) {
        HASHCTL ctl;
        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(collation_cache_entry);
        collation_cache = hash_create("Collation cache", 100, &ctl, HASH_ELEM | HASH_BLOBS);
    }

    // Find or create cache entry
    cache_entry = hash_search(collation_cache, &collation, HASH_ENTER, &found);
    if (!found) {
        // Initialize new entry as invalid
        cache_entry->flags_valid = false;
        cache_entry->locale = 0;
    }

    // Set flags if requested and not already valid
    if (set_flags && !cache_entry->flags_valid) {
        // Look up collation in system catalog
        HeapTuple tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
        Form_pg_collation collform = (Form_pg_collation) GETSTRUCT(tp);

        if (collform->collprovider == COLLPROVIDER_BUILTIN) {
            // For builtin collations, check locale name
            const char *colllocale = get_collation_locale(tp);
            cache_entry->collate_is_c = true;
            cache_entry->ctype_is_c = (strcmp(colllocale, "C") == 0);
        }
        else if (collform->collprovider == COLLPROVIDER_LIBC) {
            // For libc collations, check both collate and ctype
            const char *collcollate = get_collation_collate(tp);
            const char *collctype = get_collation_ctype(tp);
            cache_entry->collate_is_c = (strcmp(collcollate, "C") == 0 || strcmp(collcollate, "POSIX") == 0);
            cache_entry->ctype_is_c = (strcmp(collctype, "C") == 0 || strcmp(collctype, "POSIX") == 0);
        }
        else {
            // Other providers default to non-C
            cache_entry->collate_is_c = false;
            cache_entry->ctype_is_c = false;
        }

        cache_entry->flags_valid = true;
        ReleaseSysCache(tp);
    }

    return cache_entry;
}
```