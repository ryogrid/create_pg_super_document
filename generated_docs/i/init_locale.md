# init_locale

## Location
src/backend/main/main.c: 303 - 321

## Overview
A utility function that safely sets a locale category to a specified value with fallback to the "C" locale if the initial setting fails.

## Definition


## Detailed Description
This function provides a robust way to initialize locale settings during PostgreSQL startup. It attempts to set the specified locale category to the desired locale value, but includes a fallback mechanism to ensure the system can always establish a valid locale setting.

The function follows a two-step approach:
1. First, it tries to set the locale category to the requested locale value
2. If that fails (e.g., due to an invalid LC_* environment variable), it falls back to the "C" locale
3. If even the "C" locale fails (potentially due to memory issues), the entire startup process is terminated with a FATAL error

This ensures that PostgreSQL always has a valid locale setting for each category, which is essential for proper string handling, collation, and internationalization support.

## Parameters / Member Variables
- : A string name of the locale category (used for error reporting)
- : The integer constant representing the locale category (e.g., LC_COLLATE, LC_CTYPE)
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
LC_ALL=: The desired locale string to set (e.g., "en_US.UTF-8", "C", or empty string for environment default)

## Dependencies
- Functions called/Symbols referenced:
  - pg_perm_setlocale (attempts to permanently set the locale for the given category)
  - elog (logs fatal error if both locale attempts fail)
- Called from:
  - main (multiple times for different locale categories during startup)

## Notes and Other Information
- This function is static and only accessible within the main.c source file
- The function guarantees that upon successful return, a valid locale setting exists for the specified category
- Used extensively during PostgreSQL startup to configure LC_COLLATE, LC_CTYPE, LC_MESSAGES, LC_MONETARY, LC_NUMERIC, and LC_TIME
- The "C" locale serves as a universal fallback that should always be available on POSIX-compliant systems
- Fatal errors from this function will prevent PostgreSQL from starting up, emphasizing the critical importance of locale configuration
- The function uses pg_perm_setlocale rather than standard setlocale() to ensure the setting persists across potential forks and environmental changes