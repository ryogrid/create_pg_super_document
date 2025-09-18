# check_locale_monetary

## Location
[src/backend/utils/adt/pg_locale.c:381-386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L381-L386)

## Overview
A GUC (Grand Unified Configuration) check hook function that validates monetary locale settings.

## Definition


## Detailed Description
This function serves as a PostgreSQL GUC check hook specifically for validating LC_MONETARY locale settings. It acts as a thin wrapper around the generic check_locale() function, specifically targeting the LC_MONETARY category. The function is part of PostgreSQL's configuration validation system and is called when the lc_monetary configuration parameter is being set or modified. It accepts empty string ("") values which represent the postmaster's environment locale setting.

## Parameters / Member Variables
- : Pointer to the new locale value being set (can be modified by the function)
- : Pointer to extra data that can be passed to the assign hook (not used in this implementation)
- : The source of the configuration change (command line, config file, etc.)

## Dependencies
- Functions called/Symbols referenced:
  -  (with LC_MONETARY category)
- Called from (representative examples):
  - Referenced in GUC_HOOKS_H (src/include/utils/guc_hooks.h:70)

## Notes and Other Information
- Returns true if the monetary locale is valid, false otherwise
- Part of PostgreSQL's GUC system for configuration parameter validation
- Does not permanently set the locale, only validates it
- The actual locale setting is deferred until the locale is actually needed
- Accepts empty string values that reference the postmaster's environment
- This is one of several locale-specific check functions for different locale categories
- The function follows PostgreSQL's lazy locale initialization pattern