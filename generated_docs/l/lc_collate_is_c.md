# lc_collate_is_c

## Location
[src/backend/utils/adt/pg_locale.c:1340-1406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L1340-L1406)

## Overview
lc_collate_is_c is a function that determines whether a given collation's LC_COLLATE property is equivalent to the C or POSIX locale, used for performance optimization in string operations.

## Definition
```c
bool lc_collate_is_c(Oid collation)
```

## Detailed Description
This function provides a fast way to determine if a collation uses C-style collation rules, which enables various optimizations in string comparison and sorting operations. It handles several special cases: returns false for invalid collation OID to trigger error handling, caches results for the default collation by querying the system locale, immediately returns true for built-in C/POSIX collations, and uses the collation cache for all other collations. The caching mechanism ensures that expensive catalog lookups and system calls are minimized while maintaining accuracy.

## Parameters / Member Variables
- `collation`: OID of the collation to check for C/POSIX equivalence

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_collation_cache](lookup_collation_cache.md)
  - setlocale
  - strcmp
  - elog
- Called from (representative examples):
  - [hashtext](../h/hashtext.md)
  - [varstr_cmp](../v/varstr_cmp.md)
  - [texteq](../t/texteq.md)
  - [text_starts_with](../t/text_starts_with.md)
  - [varstr_sortsupport](../v/varstr_sortsupport.md)

## Notes and Other Information
- Returns false for invalid collation OID (0) to force non-C code path and proper error reporting
- Uses static variable to cache result for DEFAULT_COLLATION_OID to avoid repeated system calls
- Handles different collation providers: BUILTIN (always C-equivalent), ICU (never C-equivalent), LIBC (depends on system locale)
- Critical for performance in text processing functions that can use faster algorithms when collation is C/POSIX
- Used extensively throughout the codebase for collation-aware string operations