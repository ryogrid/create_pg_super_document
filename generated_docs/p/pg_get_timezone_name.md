# pg_get_timezone_name

## Location
src/timezone/localtime.c: 1875 - 1889

## Overview
This function returns the name string of a given timezone structure.

## Definition


## Detailed Description
pg_get_timezone_name is a simple accessor function that retrieves the timezone name from a pg_tz structure. The function performs a null pointer check and returns the TZname field from the timezone structure if valid, or NULL if the timezone pointer is null.

This is a straightforward getter function that provides safe access to the timezone name without exposing the internal structure details to callers.

## Parameters / Member Variables
- : Pointer to the timezone structure whose name should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - pg_tz (timezone structure type)
- Called from (representative examples):
  - show_timezone (src/backend/commands/variable.c:395)
  - show_log_timezone (src/backend/commands/variable.c:468)
  - timetz_at_local (src/backend/utils/adt/date.c:3168)
  - pg_timezone_names (src/backend/utils/adt/datetime.c:5165)

## Notes and Other Information
- Returns a const char pointer to the timezone name string
- Returns NULL if the input timezone pointer is null, providing null-safety
- The returned string should not be modified by the caller
- Accesses the TZname field directly from the pg_tz structure
- Simple one-line implementation with null check for defensive programming
- Located in src/timezone/localtime.c:1875-1889