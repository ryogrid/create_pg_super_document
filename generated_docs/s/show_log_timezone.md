# show_log_timezone

## Location
src/backend/commands/variable.c: 463 - 484

## Overview
This is a GUC show hook function that returns the current value of the log_timezone configuration parameter as a displayable string in PostgreSQL.

## Definition


## Detailed Description
The  function serves as the display hook for PostgreSQL's  configuration parameter. It converts the internal  global variable (a pg_tz structure) into a human-readable string representation that can be shown to users when they query the current timezone setting.

The function always returns the canonical name of the timezone zone rather than any alias that might have been used when setting it. If the timezone name cannot be determined, it returns "unknown" as a fallback.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_get_timezone_name](../p/pg_get_timezone_name.md)
- Called from (representative examples):
  - PostgreSQL GUC system (via function pointer in GUC_HOOKS_H)

## Notes and Other Information
- Always returns the canonical timezone name, not aliases
- Returns "unknown" if the timezone name cannot be determined
- Part of the standard GUC show hook pattern in PostgreSQL
- Used when users query the current log_timezone setting (e.g., SHOW log_timezone)