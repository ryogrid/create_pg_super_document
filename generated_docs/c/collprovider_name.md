# collprovider_name

## Location
[src/include/catalog/pg_collation.h:76-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/pg_collation.h#L76-L106)

## Overview
A static inline utility function that converts collation provider character codes to their corresponding human-readable string names.

## Definition

```c
static inline const char *
collprovider_name(char c)
```
## Detailed Description
The  function provides a simple mapping from single-character collation provider codes to their descriptive string names. PostgreSQL uses character codes internally to identify different collation providers, and this function translates those codes into readable names for display and debugging purposes. The function handles the three main collation providers supported by PostgreSQL: builtin, ICU, and libc.

## Parameters / Member Variables
- : A character code representing the collation provider type. Expected values are:

## Dependencies
- Functions called/Symbols referenced:
  - COLLPROVIDER_BUILTIN
  - COLLPROVIDER_ICU
  - COLLPROVIDER_LIBC
- Called from (representative examples):
  - [createdb](createdb.md)
  - [setlocales](../s/setlocales.md)
  - [setup_locale_encoding](../s/setup_locale_encoding.md)

## Notes and Other Information
- Returns "???" for any unrecognized provider code, providing a safe fallback
- Defined as a static inline function in the header file for efficient access
- Part of the PostgreSQL collation system infrastructure
- The function is commonly used during database creation and locale setup operations