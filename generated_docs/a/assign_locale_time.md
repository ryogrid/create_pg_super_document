# assign_locale_time

## Location
[src/backend/utils/adt/pg_locale.c:411-426](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L411-L426)

## Overview
A GUC (Grand Unified Configuration) assign hook function that invalidates cached LC_TIME locale information when the time locale setting is changed.

## Definition
```c
void assign_locale_time(const char *newval, void *extra)
```

## Detailed Description
This function serves as an assignment hook in PostgreSQL's GUC system for the LC_TIME locale configuration. When the time locale setting is successfully validated and assigned, this function is called to invalidate the cached time-specific locale information by setting `CurrentLCTimeValid` to false. This ensures that any cached locale-specific time formatting information (such as month names, day names, date formats, time formats, and other temporal formatting patterns) is refreshed the next time it's needed, reflecting the new time locale settings.

Unlike the monetary and numeric locale assignment functions which invalidate `CurrentLocaleConvValid`, this function specifically invalidates `CurrentLCTimeValid`, indicating that PostgreSQL maintains separate caches for different types of locale information to optimize performance while ensuring accuracy when locale settings change.

## Parameters / Member Variables
- `newval`: The new locale string value being assigned (not used in the function body but required by GUC hook interface)
- `extra`: Additional data passed by the GUC system (not used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - CurrentLCTimeValid (static boolean variable specific to time locale caching)
- Called from (representative examples):
  - GUC system when LC_TIME configuration is assigned

## Notes and Other Information
- This function is registered as a GUC assign hook in the PostgreSQL configuration system
- The function only invalidates the time-specific locale cache; actual locale information refresh happens lazily when needed
- Works in conjunction with `check_locale_time` which validates the new locale value before assignment
- Part of a broader locale management system that includes similar functions for monetary and numeric locales
- Uses a separate cache validity flag (`CurrentLCTimeValid`) distinct from the general locale conversion cache
- The LC_TIME locale setting affects formatting of dates, times, month names, day names, and other temporal elements throughout the database system

## Simplified Source

```c
void assign_locale_time(const char *newval, void *extra) {
    // Mark cached LC_TIME locale info as invalid
    // Forces refresh of time formatting when next needed
    CurrentLCTimeValid = false;
}
```