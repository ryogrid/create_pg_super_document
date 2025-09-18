# save_global_locale

## Location
src/bin/initdb/initdb.c: 362 - 385

## Overview
Saves a copy of the current global locale's name for a given category, with proper handling for non-ASCII characters on Windows systems.

## Definition


## Detailed Description
This function creates a backup of the current global locale setting for the specified category. It's designed to work in conjunction with  to implement a save-restore pattern for locale management. The function uses platform-specific approaches: on Windows, it uses the wide-character variant  to handle non-ASCII locale names that might not survive encoding conversions during locale switches. On other platforms, it uses the standard  function. The returned locale name is dynamically allocated and must be freed by the caller.

## Parameters / Member Variables
- : The locale category to save (e.g., LC_ALL, LC_COLLATE, LC_CTYPE, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - save_locale_t (type definition)
  - setlocale (POSIX locale function)
  - _wsetlocale (Windows wide-character locale function)
  - wcsdup (Windows wide-character string duplication)
  - [pg_strdup](../p/pg_strdup.md) (PostgreSQL string duplication utility)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL fatal error reporting)
- Called from (representative examples):
  - [locale_date_order](../l/locale_date_order.md) (in src/bin/initdb/initdb.c:2138)
  - [check_locale_name](../c/check_locale_name.md) (in src/bin/initdb/initdb.c:2196)

## Notes and Other Information
- This function is part of initdb's locale management system
- The Windows-specific implementation prevents encoding issues when locale names contain non-ASCII characters
- The returned value must be passed to  to complete the save-restore cycle
- Memory allocation is performed for the returned locale name, requiring proper cleanup
- Failure cases result in fatal errors rather than recoverable conditions