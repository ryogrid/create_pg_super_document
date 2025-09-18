# pg_strxfrm_prefix_enabled

## Location
[src/backend/utils/adt/pg_locale.c:2463-2489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L2463-L2489)

## Overview
Determines whether the given locale provider supports PostgreSQL's prefix-based string transformation functions (pg_strxfrm_prefix and pg_strnxfrm_prefix).

## Definition
```c
bool pg_strxfrm_prefix_enabled(pg_locale_t locale)
```

## Detailed Description
This function checks whether the collation provider supports partial sort key generation through prefix transformation functions. Unlike the general pg_strxfrm_enabled() function, this specifically tests support for prefix-based transformations that can generate partial sort keys:

- **LIBC provider**: Always returns false, as standard libc does not provide prefix transformation capabilities
- **ICU provider**: Always returns true, as ICU provides ucol_nextSortKeyPart() for incremental sort key generation
- **Other providers**: Triggers an error as they should not occur in normal operation

Prefix transformations are more advanced than full transformations, allowing PostgreSQL to generate partial sort keys that can be useful for optimizations like abbreviated keys in sorting operations.

## Parameters / Member Variables
- `locale`: PostgreSQL locale structure containing collation provider information. Can be NULL (treated as LIBC provider).

## Dependencies
- Functions called/Symbols referenced:
  - COLLPROVIDER_LIBC
  - COLLPROVIDER_ICU
  - PGLOCALE_SUPPORT_ERROR
- Called from (representative examples):
  - [varstr_abbrev_convert](../v/varstr_abbrev_convert.md)
  - Sorting optimization routines

## Notes and Other Information
- This capability check is more restrictive than pg_strxfrm_enabled() since prefix transformations require more advanced collation support
- Only ICU currently supports prefix-based sort key generation in PostgreSQL
- The function enables PostgreSQL to utilize advanced sorting optimizations when ICU collations are available
- Prefix transformations allow for more efficient string comparisons by generating only the necessary portion of the sort key
- Critical for determining whether abbreviated key optimizations can be used in sorting operations