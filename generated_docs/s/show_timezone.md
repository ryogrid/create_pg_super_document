# show_timezone

## Location
src/backend/commands/variable.c: 390 - 415

## Overview
A GUC (Grand Unified Configuration) display hook function that returns the canonical name of the current session timezone for user display.

## Definition


## Detailed Description
The  function serves as a GUC show hook that provides a human-readable representation of the current session's timezone setting. It retrieves the canonical name of the timezone currently stored in the  global variable and returns it as a string suitable for display to users.

The function always attempts to return the canonical timezone name (such as "America/New_York", "UTC", or "+05:00") rather than any abbreviated or alternative forms. If for some reason the timezone name cannot be determined, it returns the string "unknown" as a fallback.

This function is called by the GUC system when users query the current timezone setting through commands like  or by accessing the  configuration parameter.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_get_timezone_name](../p/pg_get_timezone_name.md): Function that retrieves the canonical name from a pg_tz timezone object
  - session_timezone: Global variable containing the current session's timezone object

- Called from (representative examples):
  - GUC system when displaying current timezone setting
  -  SQL command
  - Configuration parameter queries

## Notes and Other Information
- Always returns the canonical timezone name when possible for consistency
- Returns "unknown" as a safe fallback if timezone name cannot be determined
- The returned string is managed by the timezone system and should not be freed by the caller
- This is part of PostgreSQL's GUC (Grand Unified Configuration) system architecture where show hooks provide user-friendly display of configuration values
- The function provides read-only access to the current timezone setting without modifying any state