# pg_stat_get_backend_activity

## Location
[src/backend/utils/adt/pgstatfuncs.c:741-765](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L741-L765)

## Overview
Returns the current activity (SQL command string) being executed by a specific PostgreSQL backend process, with appropriate permission checking and formatting.

## Definition
```c
Datum pg_stat_get_backend_activity(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the current activity string for a PostgreSQL backend process identified by its process number. It accesses the shared memory backend status array to find the backend entry and returns the st_activity_raw field, which contains the current SQL command or activity description. The function implements several security and formatting features: it checks user permissions before returning sensitive activity information, handles cases where no backend exists or activity tracking is disabled, and properly clips/formats the activity string for display. The activity string represents what the backend is currently executing or its current state.

## Parameters / Member Variables
- `procNumber` (int32): The process number identifying which backend's activity to retrieve. This is an index into the backend status array.

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_beentry_by_proc_number](pgstat_get_beentry_by_proc_number.md): Retrieves backend status entry by process number
  - [PgBackendStatus](../P/PgBackendStatus.md): Structure containing backend status information including st_activity_raw
  - HAS_PGSTAT_PERMISSIONS: Macro checking if current user has permissions to view statistics
  - [pgstat_clip_activity](pgstat_clip_activity.md): Function to properly clip/truncate activity strings for display
  - [cstring_to_text](../c/cstring_to_text.md): Converts C string to PostgreSQL text type
  - PG_GETARG_INT32: Macro to extract int32 argument from function call
  - PG_RETURN_TEXT_P: Macro to return text value from PostgreSQL function
  - [pfree](pfree.md): Memory deallocation function

## Notes and Other Information
- Returns different messages based on various conditions:
  - "<backend information not available>" if no backend exists with the specified process number
  - "<insufficient privilege>" if the current user lacks permission to view the activity
  - "<command string not enabled>" if activity tracking is disabled (track_activities = off)
  - The actual activity string if all conditions are met
- Implements permission checking using HAS_PGSTAT_PERMISSIONS macro, which verifies the user has ROLE_PG_READ_ALL_STATS or is the same user as the backend being queried
- Uses pgstat_clip_activity to ensure proper truncation of multi-byte character sequences in activity strings
- Commonly used by system monitoring views like pg_stat_activity to display current backend activities
- Essential for database monitoring, debugging, and performance analysis
- Located in src/backend/utils/adt/pgstatfuncs.c:741-765

## Simplified Source

```c
Datum
pg_stat_get_backend_activity(PG_FUNCTION_ARGS)
{
    int32 proc_number = PG_GETARG_INT32(0);
    PgBackendStatus *backend_entry;
    const char *activity_string;
    char *clipped_activity;
    text *result;

    // Get backend status entry for the specified process number
    backend_entry = pgstat_get_beentry_by_proc_number(proc_number);

    // Determine activity string based on backend status and permissions
    if (backend_entry == NULL)
        activity_string = "<backend information not available>";
    else if (!HAS_PGSTAT_PERMISSIONS(backend_entry->st_userid))
        activity_string = "<insufficient privilege>";
    else if (*(backend_entry->st_activity_raw) == '\0')
        activity_string = "<command string not enabled>";
    else
        activity_string = backend_entry->st_activity_raw;

    // Properly clip activity string and convert to PostgreSQL text type
    clipped_activity = pgstat_clip_activity(activity_string);
    result = cstring_to_text(activity_string);
    pfree(clipped_activity);

    PG_RETURN_TEXT_P(result);
}
```