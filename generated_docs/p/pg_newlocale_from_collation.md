# pg_newlocale_from_collation

## Location
src/backend/utils/adt/pg_locale.c: 1574 - 1751

## Overview
Creates and caches a PostgreSQL locale object from a collation OID, supporting multiple collation providers (builtin, libc, ICU) with lifetime caching and version validation.

## Definition


## Detailed Description
This function creates a pg_locale_t object from a collation OID, implementing a comprehensive caching mechanism for the lifetime of the backend session. It handles three different collation providers: builtin, libc, and ICU, each with specific initialization requirements. As a special optimization, the default/database collation returns 0 for libc providers. The function validates collation versions to detect mismatches between the database catalog and the operating system, issuing warnings when version conflicts are detected. For libc collations, it handles both simple cases (where collate and ctype are the same) and complex cases (where they differ), with platform-specific implementations for Windows and Unix-like systems.

## Parameters / Member Variables
- `collid`: The OID of the collation to create a locale object from, must be a valid OID

## Dependencies
- Functions called/Symbols referenced:
  - lookup_collation_cache
  - SearchSysCache1, SysCacheGetAttrNotNull, SysCacheGetAttr, ReleaseSysCache
  - builtin_validate_locale, GetDatabaseEncoding
  - newlocale, _create_locale (platform-specific)
  - make_icu_collator
  - get_collation_actual_version
  - report_newlocale_failure
  - MemoryContextStrdup, MemoryContextAlloc
  - TextDatumGetCString
  - quote_qualified_identifier, get_namespace_name
- Called from (representative examples):
  - hashtext (at line 281)
  - DefineCollation (at line 386)
  - str_tolower, str_toupper, str_initcap (in formatting functions)
  - varstr_cmp (at line 1561)
  - text comparison functions (texteq, textne, etc.)

## Notes and Other Information
- Results are cached for the lifetime of the backend session to avoid repeated expensive locale creation
- Supports three collation providers: COLLPROVIDER_BUILTIN, COLLPROVIDER_LIBC, and COLLPROVIDER_ICU
- Implements version checking to detect collation version mismatches between catalog and system
- For libc provider, handles different collate/ctype combinations on Unix but not on Windows
- Memory allocation is done in TopMemoryContext for session-lifetime persistence
- Default collation (DEFAULT_COLLATION_OID) receives special handling for performance optimization
- Includes comprehensive error handling for various failure scenarios including locale creation failures and version mismatches