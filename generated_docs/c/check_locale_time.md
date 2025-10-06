# check_locale_time

## Location
[src/backend/utils/adt/pg_locale.c:405-410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L405-L410)

## Overview
A GUC (Grand Unified Configuration) check hook function that validates whether a given locale string is valid for the LC_TIME locale category before assignment.

## Definition
```c
bool check_locale_time(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function serves as a validation hook in PostgreSQL's GUC system for the LC_TIME locale configuration parameter. It acts as a gatekeeper that ensures only valid time locale values are accepted before they are assigned to the system. The function delegates the actual validation logic to the generic `check_locale` function, specifically checking the LC_TIME category which controls time and date formatting rules such as date formats, time formats, month names, day names, and other temporal formatting conventions.

When a user attempts to change the LC_TIME setting via configuration files, SQL commands, or other configuration mechanisms, this function is called to validate the proposed value before any assignment occurs.

## Parameters / Member Variables
- `newval`: Pointer to a pointer containing the new locale string value to validate
- `extra`: Pointer to store any additional data for the GUC system (unused in this implementation)
- `source`: The source of the configuration change (e.g., configuration file, SQL command, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [check_locale](check_locale.md) (validates locale using LC_TIME category)
  - GucSource (enum type for configuration sources)
- Called from (representative examples):
  - GUC system when LC_TIME configuration is being validated

## Notes and Other Information
- Returns true if the locale is valid, false otherwise
- Part of PostgreSQL's locale management system that includes similar validation functions for monetary and numeric locales
- Works in conjunction with `assign_locale_time` which is called after successful validation
- The LC_TIME locale affects formatting of dates, times, month names, day names, and other temporal display elements
- Validation includes checking for ASCII-only characters and testing actual locale availability on the system
- Critical for ensuring consistent time and date formatting across the database system

## Simplified Source

```c
bool check_locale_time(char **newval, void **extra, GucSource source) {
    // Validate time locale using generic locale checker
    return check_locale(LC_TIME, *newval, NULL);
}
```