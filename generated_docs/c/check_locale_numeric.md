# check_locale_numeric

## Location
[src/backend/utils/adt/pg_locale.c:393-398](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L393-L398)

## Overview
A GUC (Grand Unified Configuration) check hook function that validates whether a given locale string is valid for the LC_NUMERIC locale category before assignment.

## Definition
```c
bool check_locale_numeric(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function serves as a validation hook in PostgreSQL's GUC system for the LC_NUMERIC locale configuration parameter. It acts as a gatekeeper that ensures only valid numeric locale values are accepted before they are assigned to the system. The function delegates the actual validation logic to the generic `check_locale` function, specifically checking the LC_NUMERIC category which controls number formatting rules such as decimal separators, thousands separators, and digit grouping.

When a user attempts to change the LC_NUMERIC setting via configuration files, SQL commands, or other configuration mechanisms, this function is called to validate the proposed value before any assignment occurs.

## Parameters / Member Variables
- `newval`: Pointer to a pointer containing the new locale string value to validate
- `extra`: Pointer to store any additional data for the GUC system (unused in this implementation)
- `source`: The source of the configuration change (e.g., configuration file, SQL command, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [check_locale](check_locale.md) (validates locale using LC_NUMERIC category)
  - GucSource (enum type for configuration sources)
- Called from (representative examples):
  - GUC system when LC_NUMERIC configuration is being validated

## Notes and Other Information
- Returns true if the locale is valid, false otherwise
- Part of PostgreSQL's locale management system that includes similar validation functions for monetary and time locales
- Works in conjunction with `assign_locale_numeric` which is called after successful validation
- The LC_NUMERIC locale affects formatting of numeric values including decimal points and thousands separators
- Validation includes checking for ASCII-only characters and testing actual locale availability on the system