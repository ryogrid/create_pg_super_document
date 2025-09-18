# IsoLocaleName

## Location
src/backend/utils/adt/pg_locale.c: 1201 - 1218

## Overview
IsoLocaleName is a static function that normalizes Windows locale names to ISO standard format, specifically handling the conversion of locale names for message localization.

## Definition
```c
static char *IsoLocaleName(const char *winlocname)
```

## Detailed Description
This function serves as a locale name normalizer that converts Windows-style locale names to ISO standard format. It handles two special cases: "c" and "posix" locale names are converted to the canonical "C" locale. For all other locale names, it delegates to the `get_iso_localename` function which performs the actual ISO conversion. The function maintains a static buffer for the result, making it suitable for repeated calls within the same process context.

## Parameters / Member Variables
- `winlocname`: Input Windows locale name string to be normalized

## Dependencies
- Functions called/Symbols referenced:
  - [get_iso_localename](../g/get_iso_localename.md)
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - strcpy
- Called from (representative examples):
  - collation_cache_entry
  - [pg_perm_setlocale](../p/pg_perm_setlocale.md)

## Notes and Other Information
- Returns a pointer to a static buffer, so the result should be used immediately or copied
- Special handling for "c" and "posix" locale names ensures consistent behavior across platforms
- Part of PostgreSQL's locale management system for proper internationalization support