# TimestampTimestampTzRequiresRewrite

## Location
src/backend/utils/adt/timestamp.c: 6273 - 6285

## Overview
Determines whether conversions between timestamp and timestamptz types require rewriting data based on the current TimeZone GUC setting.

## Definition


## Detailed Description
This function checks if the current TimeZone GUC setting would cause timestamp_timestamptz and timestamptz_timestamp conversion functions to be no-ops (where the return value has the same bits as the argument). The function returns false only when the session timezone has a zero offset from UTC, meaning conversions between timestamp and timestamptz would not change the actual stored value. This information is used to optimize table alterations and comparisons by avoiding unnecessary data rewrites when the timezone offset is zero.

The function follows PostgreSQL's convention of assuming GUC changes occur no more often than STABLE functions change, so the returned answer remains valid for that duration.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - pg_get_timezone_offset
  - session_timezone
- Called from (representative examples):
  - ATColumnChangeRequiresRewrite (in src/backend/commands/tablecmds.c:13126)
  - timestamptz_cmp_internal (in src/include/utils/timestamp.h:145)

## Notes and Other Information
- This function is used for optimization purposes to avoid unnecessary table rewrites during ALTER TABLE operations
- Returns false only when the current session timezone has exactly zero offset from UTC
- The function is designed to be stable within the scope of GUC setting changes
- Critical for performance optimization in timestamp/timestamptz conversions
- Located in src/backend/utils/adt/timestamp.c:6273-6285
- Used by table alteration commands to determine if column type changes require physical data rewriting