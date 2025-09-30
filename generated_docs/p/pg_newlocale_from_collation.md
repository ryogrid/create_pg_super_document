# pg_newlocale_from_collation

## Location
[src/backend/utils/adt/pg_locale.c:1574-1751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L1574-L1751)

## Overview
Creates and caches a PostgreSQL locale object from a collation OID, supporting multiple collation providers (builtin, libc, ICU) with lifetime caching and version validation.

## Definition

```c
struct pg_locale_struct result;
```
## Detailed Description
This function creates a pg_locale_t object from a collation OID, implementing a comprehensive caching mechanism for the lifetime of the backend session. It handles three different collation providers: builtin, libc, and ICU, each with specific initialization requirements. As a special optimization, the default/database collation returns 0 for libc providers. The function validates collation versions to detect mismatches between the database catalog and the operating system, issuing warnings when version conflicts are detected. For libc collations, it handles both simple cases (where collate and ctype are the same) and complex cases (where they differ), with platform-specific implementations for Windows and Unix-like systems.

## Parameters / Member Variables
- `collid`: The OID of the collation to create a locale object from, must be a valid OID

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_collation_cache](../l/lookup_collation_cache.md)
  - [SearchSysCache1](../S/SearchSysCache1.md), SysCacheGetAttrNotNull, SysCacheGetAttr, ReleaseSysCache
  - [builtin_validate_locale](../b/builtin_validate_locale.md), GetDatabaseEncoding
  - newlocale, _create_locale (platform-specific)
  - [make_icu_collator](../m/make_icu_collator.md)
  - [get_collation_actual_version](../g/get_collation_actual_version.md)
  - [report_newlocale_failure](../r/report_newlocale_failure.md)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md), MemoryContextAlloc
  - TextDatumGetCString
  - [quote_qualified_identifier](../q/quote_qualified_identifier.md), get_namespace_name
- Called from (representative examples):
  - [hashtext](../h/hashtext.md) (at line 281)
  - [DefineCollation](../D/DefineCollation.md) (at line 386)
  - [str_tolower](../s/str_tolower.md), str_toupper, str_initcap (in formatting functions)
  - [varstr_cmp](../v/varstr_cmp.md) (at line 1561)
  - [text](../t/text.md) comparison functions (texteq, textne, etc.)

## Notes and Other Information
- Results are cached for the lifetime of the backend session to avoid repeated expensive locale creation
- Supports three collation providers: COLLPROVIDER_BUILTIN, COLLPROVIDER_LIBC, and COLLPROVIDER_ICU
- Implements version checking to detect collation version mismatches between catalog and system
- For libc provider, handles different collate/ctype combinations on Unix but not on Windows
- Memory allocation is done in TopMemoryContext for session-lifetime persistence
- Default collation (DEFAULT_COLLATION_OID) receives special handling for performance optimization
- Includes comprehensive error handling for various failure scenarios including locale creation failures and version mismatches

## Simplified Source

```c
pg_locale_t pg_newlocale_from_collation(Oid collid) {
    collation_cache_entry *cache_entry;

    Assert(OidIsValid(collid));

    // Handle default collation specially
    if (collid == DEFAULT_COLLATION_OID) {
        if (default_locale.provider == COLLPROVIDER_LIBC)
            return (pg_locale_t) 0;  // Optimization for default libc
        else
            return &default_locale;
    }

    // Look up in cache
    cache_entry = lookup_collation_cache(collid, false);

    // If not cached, create new locale
    if (cache_entry->locale == 0) {
        HeapTuple tp;
        Form_pg_collation collform;
        struct pg_locale_struct result;
        pg_locale_t resultp;

        // Get collation info from catalog
        tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
        if (!HeapTupleIsValid(tp))
            elog(ERROR, "cache lookup failed for collation %u", collid);
        collform = (Form_pg_collation) GETSTRUCT(tp);

        // Initialize result structure
        memset(&result, 0, sizeof(result));
        result.provider = collform->collprovider;
        result.deterministic = collform->collisdeterministic;

        // Handle different collation providers
        if (collform->collprovider == COLLPROVIDER_BUILTIN) {
            // Set up builtin locale
            Datum datum = SysCacheGetAttrNotNull(COLLOID, tp, Anum_pg_collation_colllocale);
            const char *locstr = TextDatumGetCString(datum);
            builtin_validate_locale(GetDatabaseEncoding(), locstr);
            result.info.builtin.locale = MemoryContextStrdup(TopMemoryContext, locstr);

        } else if (collform->collprovider == COLLPROVIDER_LIBC) {
            // Create libc locale using newlocale/create_locale
            // Handle platform differences and collate/ctype combinations
            // (Platform-specific locale creation logic)

        } else if (collform->collprovider == COLLPROVIDER_ICU) {
            // Set up ICU collator
            const char *iculocstr = /* get locale string */;
            const char *icurules = /* get optional rules */;
            make_icu_collator(iculocstr, icurules, &result);
        }

        // Validate collation version if present
        // (Version checking and warning logic)

        ReleaseSysCache(tp);

        // Allocate and cache the result
        resultp = MemoryContextAlloc(TopMemoryContext, sizeof(*resultp));
        *resultp = result;
        cache_entry->locale = resultp;
    }

    return cache_entry->locale;
}
```