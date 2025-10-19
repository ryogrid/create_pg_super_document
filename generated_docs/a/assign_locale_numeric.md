# assign_locale_numeric

## Location
[src/backend/utils/adt/pg_locale.c:399-404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L399-L404)

## Overview
A GUC (Grand Unified Configuration) assign hook function that invalidates cached locale conversion information when the LC_NUMERIC locale setting is changed.

## Definition
```c
void assign_locale_numeric(const char *newval, void *extra)
```

## Detailed Description
This function serves as an assignment hook in PostgreSQL's GUC system for the LC_NUMERIC locale configuration. When the numeric locale setting is successfully validated and assigned, this function is called to invalidate the cached locale conversion information by setting `CurrentLocaleConvValid` to false. This ensures that any cached locale-specific formatting information (such as decimal separators, thousands separators, digit grouping patterns) is refreshed the next time it's needed, reflecting the new numeric locale settings.

The function is part of PostgreSQL's locale management system, which maintains cached locale information for performance reasons but must invalidate this cache whenever locale settings change. The LC_NUMERIC locale affects how numbers are formatted and displayed throughout the database system.

## Parameters / Member Variables
- `newval`: The new locale string value being assigned (not used in the function body but required by GUC hook interface)
- `extra`: Additional data passed by the GUC system (not used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - CurrentLocaleConvValid (static boolean variable)
- Called from (representative examples):
  - GUC system when LC_NUMERIC configuration is assigned

## Notes and Other Information
- This function is registered as a GUC assign hook in the PostgreSQL configuration system
- The function only invalidates the cache; actual locale information refresh happens lazily when needed
- Works in conjunction with `check_locale_numeric` which validates the new locale value before assignment
- Part of a broader locale management system that includes similar functions for monetary and time locales
- The LC_NUMERIC locale setting affects decimal points, thousands separators, and digit grouping in number formatting

## Simplified Source

```c
void assign_locale_numeric(const char *newval, void *extra) {
    // Mark cached locale conversion info as invalid
    // Forces refresh of numeric formatting when next needed
    CurrentLocaleConvValid = false;
}
```