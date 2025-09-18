# pg_strxfrm_enabled

## Location
[src/backend/utils/adt/pg_locale.c:2372-2403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L2372-L2403)

## Overview
Determines whether the given locale provider supports PostgreSQL's string transformation functions (pg_strxfrm and pg_strnxfrm) for generating sort keys.

## Definition
```c
bool pg_strxfrm_enabled(pg_locale_t locale)
```

## Detailed Description
This function serves as a capability check for sort key generation functionality across different collation providers. It implements PostgreSQL's conservative approach to dealing with platform-specific strxfrm() reliability issues:

- **LIBC provider**: Returns false by default due to known inconsistencies between strcoll() and strxfrm() on many platforms (glibc versions). Can be overridden by defining TRUST_STRXFRM compile-time flag.
- **ICU provider**: Always returns true, as ICU's sort key generation is considered reliable and consistent.
- **Other providers**: Triggers an error as they should not occur in normal operation.

The function helps PostgreSQL's collation system decide whether to use optimized sort key-based comparisons or fall back to direct string comparison methods.

## Parameters / Member Variables
- `locale`: PostgreSQL locale structure containing collation provider information. Can be NULL (treated as LIBC provider).

## Dependencies
- Functions called/Symbols referenced:
  - COLLPROVIDER_LIBC
  - COLLPROVIDER_ICU
  - PGLOCALE_SUPPORT_ERROR
- Called from (representative examples):
  - [varstr_sortsupport](../v/varstr_sortsupport.md)
  - Various sorting and indexing operations

## Notes and Other Information
- This function addresses documented reliability issues with glibc's strxfrm() implementation
- The TRUST_STRXFRM preprocessor flag allows advanced users to override the conservative default for LIBC collations
- ICU provider is always trusted because no similar reliability issues have been identified
- The function is part of PostgreSQL's strategy to provide consistent collation behavior across different platforms
- Critical for determining whether sort support can use optimized transformation-based comparisons