# pgstat_get_io_object_name

## Location
[src/backend/utils/activity/pgstat_io.c:240-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_io.c#L240-L254)

## Overview
Returns a human-readable string representation of PostgreSQL I/O object enumeration values.

## Definition

```c
const char *
pgstat_get_io_object_name(IOObject io_object)
```
## Detailed Description
This function provides a mapping from internal IOObject enumeration values to their corresponding string representations for display purposes. It uses a switch statement to convert each IOObject enum value to its descriptive string name. The function handles the defined I/O object types including regular relations and temporary relations. If an unrecognized IOObject value is passed, the function logs an error and calls pg_unreachable() to indicate this should never happen in correct operation.

## Parameters / Member Variables
- `io_object`: An IOObject enumeration value to be converted to its string representation

## Dependencies
- Functions called/Symbols referenced:
  - [IOObject](../I/IOObject.md)
  - IOOBJECT_RELATION
  - IOOBJECT_TEMP_RELATION
  - pg_unreachable
- Called from (representative examples):
  - [pg_stat_get_io](pg_stat_get_io.md)

## Notes and Other Information
- Returns constant string literals for each I/O object type
- The mapping includes: "relation", "temp relation"
- Uses pg_unreachable() to handle impossible code paths for defensive programming
- This function is primarily used for displaying I/O statistics in a user-friendly format
- Located in src/backend/utils/activity/pgstat_io.c:240-254

## Simplified Source

```c
const char *
pgstat_get_io_object_name(IOObject io_object)
{
    // Convert IO object enum to human-readable string
    switch (io_object)
    {
        case IOOBJECT_RELATION:
            return "relation";
        case IOOBJECT_TEMP_RELATION:
            return "temp relation";
    }

    // Should never reach here with valid input
    elog(ERROR, "unrecognized IOObject value: %d", io_object);
    pg_unreachable();
}
```