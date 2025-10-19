# assign_locale_monetary

## Location
[src/backend/utils/adt/pg_locale.c:387-392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L387-L392)

## Overview
A GUC (Grand Unified Configuration) assign hook function that invalidates cached locale conversion information when the LC_MONETARY locale setting is changed.

## Definition
```c
void assign_locale_monetary(const char *newval, void *extra)
```

## Detailed Description
This function serves as an assignment hook in PostgreSQL's GUC system for the LC_MONETARY locale configuration. When the monetary locale setting is successfully validated and assigned, this function is called to invalidate the cached locale conversion information by setting `CurrentLocaleConvValid` to false. This ensures that any cached locale-specific formatting information (such as currency symbols, decimal points, etc.) is refreshed the next time it's needed, reflecting the new monetary locale settings.

The function is part of PostgreSQL's locale management system, which maintains cached locale information for performance reasons but must invalidate this cache whenever locale settings change.

## Parameters / Member Variables
- `newval`: The new locale string value being assigned (not used in the function body but required by GUC hook interface)
- `extra`: Additional data passed by the GUC system (not used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - CurrentLocaleConvValid (static boolean variable)
- Called from (representative examples):
  - GUC system when LC_MONETARY configuration is assigned

## Notes and Other Information
- This function is registered as a GUC assign hook in the PostgreSQL configuration system
- The function only invalidates the cache; actual locale information refresh happens lazily when needed
- Works in conjunction with `check_locale_monetary` which validates the new locale value before assignment
- Part of a broader locale management system that includes similar functions for numeric and time locales

## Simplified Source

```c
void assign_locale_monetary(const char *newval, void *extra) {
    // Mark cached locale conversion info as invalid
    // Forces refresh of monetary formatting when next needed
    CurrentLocaleConvValid = false;
}
```