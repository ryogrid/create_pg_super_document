# setlocales

## Location
[src/bin/initdb/initdb.c:2406-2496](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2406-L2496)

## Overview
Sets up and validates locale variables during PostgreSQL database initialization, handling different locale providers (libc, builtin, ICU) and canonicalizing locale names.

## Definition

```c
static void
setlocales(void)
```
## Detailed Description
This function is responsible for configuring all locale-related settings during database cluster initialization (initdb). It performs several key operations:

1. **Default locale assignment**: If a general LANG=C.UTF-8
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
LC_ALL= is specified, it becomes the default for any unspecified locale categories (LC_CTYPE, LC_COLLATE, LC_NUMERIC, LC_TIME, LC_MONETARY, LC_MESSAGES, and datlocale).

2. **Locale canonicalization**: Uses  to validate and canonicalize each locale category, obtaining missing values from the current environment.

3. **Provider-specific handling**:
   - **LIBC provider**: Uses standard system locales
   - **Builtin provider**: Only accepts "C" and "C.UTF-8" locales
   - **ICU provider**: Converts locale names to ICU language tags and validates them

4. **Special platform handling**: On platforms without LC_MESSAGES support (like Windows), falls back to using LC_CTYPE settings for messages.

The function ensures that all locale settings are valid and properly formatted for the chosen collation provider before database creation proceeds.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables:
- LANG=C.UTF-8
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
LC_ALL=: General locale setting that serves as default for other categories
- , , , , , : Specific locale category variables
- : Locale used for database-level collation and character classification
- : The collation provider (LIBC, BUILTIN, or ICU)

## Dependencies
- Functions called/Symbols referenced:
  - : Validates and canonicalizes locale names
  - : Returns string name of collation provider
  - : Converts locale to ICU language tag format
  - : Validates ICU locale strings
  - : PostgreSQL memory deallocation function
  - : Fatal error reporting function

- Called from (representative examples):
  - : Part of the locale and encoding setup process during initdb

## Notes and Other Information
- Assumes  has already been called via 
- For ICU provider, the function converts locale names to language tags and provides user feedback about the conversion
- Builtin provider only supports C and C.UTF-8 locales, making it suitable for deterministic collation behavior
- The function will terminate initdb with a fatal error if ICU support is required but not compiled in
- Platform-specific behavior for LC_MESSAGES handling on Windows systems
- Essential part of the database cluster initialization process, ensuring locale consistency across the cluster