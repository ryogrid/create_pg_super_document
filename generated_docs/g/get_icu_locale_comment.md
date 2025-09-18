# get_icu_locale_comment

## Location
src/backend/commands/collationcmds.c: 650 - 697

## Overview
Retrieves a human-readable display name (comment) for an ICU locale, ensuring the result contains only ASCII characters suitable for template0 database storage.

## Definition
```c
static char *get_icu_locale_comment(const char *localename)
```

## Detailed Description
This function obtains a localized display name for a given ICU locale identifier using the ICU library's uloc_getDisplayName function. The display name is retrieved in English ("en" locale) to provide a consistent, human-readable description of the locale. The function enforces a strict ASCII-only policy by rejecting any display names containing characters with values greater than 127, as template0 database contents must be encoding-agnostic. If successful, it returns a palloc'd string containing the ASCII display name; otherwise, it returns NULL.

## Parameters / Member Variables
- `localename`: The ICU locale identifier string for which to retrieve the display name

## Dependencies
- Functions called/Symbols referenced:
  - uloc_getDisplayName (ICU library function)
  - lengthof (PostgreSQL macro)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - U_ZERO_ERROR (ICU constant)
  - U_FAILURE (ICU macro)
- Called from (representative examples):
  - [pg_import_system_collations](../p/pg_import_system_collations.md)

## Notes and Other Information
- This is a static function with internal linkage, only visible within collationcmds.c
- Uses ICU library functions, so it's only available when PostgreSQL is built with ICU support
- The 128-character buffer size for display names should be sufficient for most locale names
- The ASCII-only restriction is critical for template0 compatibility across different database encodings
- Returns NULL on any error condition rather than raising exceptions, allowing the caller to handle missing comments gracefully
- Part of PostgreSQL's collation import system for creating user-friendly collation descriptions