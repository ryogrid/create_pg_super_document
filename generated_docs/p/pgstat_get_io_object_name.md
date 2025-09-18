# pgstat_get_io_object_name

## Location
src/backend/utils/activity/pgstat_io.c: 240 - 254

## Overview
Returns a human-readable string representation of PostgreSQL I/O object enumeration values.

## Definition


## Detailed Description
This function provides a mapping from internal IOObject enumeration values to their corresponding string representations for display purposes. It uses a switch statement to convert each IOObject enum value to its descriptive string name. The function handles the defined I/O object types including regular relations and temporary relations. If an unrecognized IOObject value is passed, the function logs an error and calls pg_unreachable() to indicate this should never happen in correct operation.

## Parameters / Member Variables
- `io_object`: An IOObject enumeration value to be converted to its string representation

## Dependencies
- Functions called/Symbols referenced:
  - IOObject
  - IOOBJECT_RELATION
  - IOOBJECT_TEMP_RELATION
  - pg_unreachable
- Called from (representative examples):
  - pg_stat_get_io

## Notes and Other Information
- Returns constant string literals for each I/O object type
- The mapping includes: "relation", "temp relation"
- Uses pg_unreachable() to handle impossible code paths for defensive programming
- This function is primarily used for displaying I/O statistics in a user-friendly format
- Located in src/backend/utils/activity/pgstat_io.c:240-254