# check_locale_name

## Location
src/bin/initdb/initdb.c: 2184 - 2246

## Overview
Validates that a locale name is valid for a specific locale category and optionally returns the canonical name of the locale.

## Definition


## Detailed Description
This function verifies that a given locale name is valid for the specified locale category by attempting to set it using setlocale(). It serves as a validation mechanism during PostgreSQL database initialization to ensure that requested locales are supported by the system. The function also provides the ability to retrieve the canonical (normalized) form of the locale name, which is particularly useful for resolving environment-based locale specifications (empty string).

The function implements several safety measures including ASCII-only validation on Windows platforms and provides detailed error messages with hints for common configuration issues. It matches the behavior of the backend's check_locale() function to ensure consistency across PostgreSQL components.

## Parameters / Member Variables
- : The locale category to test (e.g., LC_CTYPE, LC_COLLATE, LC_TIME)
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
LC_ALL=: The locale name string to validate (NULL or empty string uses environment settings)
- : Optional output parameter; if provided, receives a malloc'd copy of the canonical locale name

## Dependencies
- Functions called/Symbols referenced:
  - pg_is_ascii (validates ASCII-only locale names on Windows)
  - save_global_locale (saves current locale state)
  - setlocale (attempts to set the locale for validation)
  - restore_global_locale (restores original locale state)
  - pg_strdup (duplicates the canonical locale name string)
  - pg_log_error (logs error messages)
  - pg_log_error_hint (provides helpful error hints)
  - pg_fatal (terminates with fatal error)
- Called from (representative examples):
  - setlocales (during locale configuration setup)

## Notes and Other Information
- Provides specific error handling for empty locale strings (environment-based configuration)
- Includes Windows-specific ASCII validation to prevent issues with non-ASCII locale names
- Offers helpful hints suggesting --icu-locale for ICU-specific locale names
- The function is locale-safe, preserving the original locale setting after validation
- Part of the initdb utility's locale configuration validation system
- Fatal errors will terminate the program if invalid locales are encountered
- The canonical name feature helps resolve environment variables like LANG and LC_*