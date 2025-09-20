# check_locale

## Location
[src/backend/utils/adt/pg_locale.c:316-380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L316-L380)

## Overview
Validates whether a locale name is valid for a specific locale category and optionally returns the canonical name of the locale.

## Definition

```c
bool
check_locale(int category, const char *locale, char **canonname)
```
## Detailed Description
This function validates locale names by attempting to set the specified locale using setlocale() and then restoring the original locale. It provides safety checks against non-ASCII locale names, which can cause issues on Windows systems. The function can optionally return the canonical form of the locale name, which is particularly useful for resolving what the empty string ("") means in locale contexts (typically the server environment value). The validation is performed by temporarily changing the locale and checking if the operation succeeds.

## Parameters / Member Variables
- : The locale category to validate against (LC_COLLATE, LC_CTYPE, LC_MESSAGES, etc.)
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
LC_ALL=: The locale name string to validate
- : Optional output parameter; if not NULL, receives a palloc'd copy of the canonical locale name upon success

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
  - 
  - 
- Called from (representative examples):
  -  (src/backend/commands/dbcommands.c:1053, 1059)
  -  (src/backend/utils/adt/pg_locale.c:383)
  -  (src/backend/utils/adt/pg_locale.c:395)
  -  (src/backend/utils/adt/pg_locale.c:407)
  -  (src/backend/utils/adt/pg_locale.c:443)

## Notes and Other Information
- Returns true if the locale is valid, false otherwise
- Rejects locale names containing non-ASCII characters to prevent Windows-specific issues
- Temporarily changes the locale to test validity, then restores the original setting
- If canonname is requested but contains non-ASCII characters, it's freed and set to NULL
- The canonical name is useful for resolving environment-based locale specifications
- Issues warnings for invalid locales and restoration failures
- Memory management: caller is responsible for freeing canonname if returned