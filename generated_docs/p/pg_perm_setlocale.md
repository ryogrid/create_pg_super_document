# pg_perm_setlocale

## Location
[src/backend/utils/adt/pg_locale.c:213-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L213-L315)

## Overview
A PostgreSQL wrapper around the standard setlocale() function that provides additional functionality for environment variable management and Windows-specific message locale handling.

## Definition

```c
char *
pg_perm_setlocale(int category, const char *locale)
```
## Detailed Description
This function wraps the libc setlocale() function with two key enhancements. First, when changing LC_CTYPE, it updates gettext's encoding for the current message domain, which is necessary for proper internationalization support especially on Windows where GNU gettext doesn't automatically track LC_CTYPE. Second, upon successful locale changes, it sets the corresponding LC_XXX environment variable to match the new setting, ensuring that subsequent setlocale(..., "") calls preserve the configuration made through this routine.

The function handles platform-specific differences, particularly for Windows where LC_MESSAGES doesn't work through the standard setlocale() call and requires special handling through environment variables and IsoLocaleName() conversion.

## Parameters / Member Variables
- : The locale category to change (LC_COLLATE, LC_CTYPE, LC_MESSAGES, LC_MONETARY, LC_NUMERIC, LC_TIME)
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL=: The locale string to set, or NULL to query current setting

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
  - 
  -  (Windows only)
  - 
- Called from (representative examples):
  -  (src/backend/main/main.c:305, 306)
  -  (src/backend/utils/adt/pg_locale.c:457)
  -  (src/backend/utils/init/postinit.c:411, 418)

## Notes and Other Information
- Returns the result of setlocale() on success, NULL on failure
- On Windows, LC_MESSAGES is handled specially since the standard setlocale() doesn't support it
- The function ensures message encoding is properly updated when LC_CTYPE changes
- Environment variables are set to preserve locale settings across process boundaries
- Uses LOCALE_NAME_BUFLEN sized buffer for saving LC_CTYPE results
- Critical for PostgreSQL's internationalization and localization support