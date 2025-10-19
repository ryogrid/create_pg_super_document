# pgstat_get_io_context_name

## Location
[src/backend/utils/activity/pgstat_io.c:221-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_io.c#L221-L239)

## Overview
Returns a human-readable string representation of PostgreSQL I/O context enumeration values.

## Definition

```c
const char *
pgstat_get_io_context_name(IOContext io_context)
```
## Detailed Description
This function provides a mapping from internal IOContext enumeration values to their corresponding string representations for display purposes. It uses a switch statement to convert each IOContext enum value to its descriptive string name. The function handles all defined I/O contexts including bulk operations, normal operations, and vacuum operations. If an unrecognized IOContext value is passed, the function logs an error and calls pg_unreachable() to indicate this should never happen in correct operation.

## Parameters / Member Variables
- `io_context`: An IOContext enumeration value to be converted to its string representation

## Dependencies
- Functions called/Symbols referenced:
  - [IOContext](../I/IOContext.md)
  - IOCONTEXT_BULKREAD
  - IOCONTEXT_BULKWRITE  
  - IOCONTEXT_NORMAL
  - IOCONTEXT_VACUUM
  - pg_unreachable
- Called from (representative examples):
  - [pg_stat_get_io](pg_stat_get_io.md)

## Notes and Other Information
- Returns constant string literals for each I/O context type
- The mapping includes: "bulkread", "bulkwrite", "normal", "vacuum"
- Uses pg_unreachable() to handle impossible code paths for defensive programming
- This function is primarily used for displaying I/O statistics in a user-friendly format
- Located in src/backend/utils/activity/pgstat_io.c:221-239

## Simplified Source

```c
const char *
pgstat_get_io_context_name(IOContext io_context)
{
    // Convert IO context enum to human-readable string
    switch (io_context)
    {
        case IOCONTEXT_BULKREAD:
            return "bulkread";
        case IOCONTEXT_BULKWRITE:
            return "bulkwrite";
        case IOCONTEXT_NORMAL:
            return "normal";
        case IOCONTEXT_VACUUM:
            return "vacuum";
    }

    // Should never reach here with valid input
    elog(ERROR, "unrecognized IOContext value: %d", io_context);
    pg_unreachable();
}
```