# pg_stat_get_db_numbackends

## Location
[src/backend/utils/adt/pgstatfuncs.c:970-989](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L970-L989)

## Overview
Returns the number of active backend processes connected to a specific database identified by its OID.

## Definition
```c
Datum pg_stat_get_db_numbackends(PG_FUNCTION_ARGS)
```

## Detailed Description
This function counts the number of active backend processes currently connected to a specific database. It iterates through all backend entries in the statistics collector and counts those that match the specified database OID. The function provides a snapshot count of active connections at the time it's called.

The counting process:
1. Fetches the total number of backend processes from the statistics collector
2. Iterates through each backend entry by index
3. Checks if each backend's database ID matches the requested database OID
4. Increments a counter for each match
5. Returns the final count

This function is commonly used by database monitoring tools and system views to track database connection usage.

## Parameters / Member Variables
- `dbid` (Oid): The database OID to count backends for

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_stat_numbackends](pgstat_fetch_stat_numbackends.md)
  - [pgstat_get_local_beentry_by_index](pgstat_get_local_beentry_by_index.md)
- Data types used:
  - [LocalPgBackendStatus](../L/LocalPgBackendStatus.md)

## Notes and Other Information
- The function provides a snapshot count and may change between calls as connections are established or terminated
- Used by system views like pg_stat_database to provide connection statistics
- The count includes all types of backend processes connected to the specified database
- No special permissions are required to call this function
- Returns 0 if no backends are connected to the specified database
- The function iterates through all backends, so performance scales with total number of connections

## Simplified Source

```c
Datum
pg_stat_get_db_numbackends(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int32 result = 0;
    int tot_backends = pgstat_fetch_stat_numbackends();

    // Count backends connected to the specified database
    for (int idx = 1; idx <= tot_backends; idx++) {
        LocalPgBackendStatus *local_beentry = pgstat_get_local_beentry_by_index(idx);

        if (local_beentry->backendStatus.st_databaseid == dbid)
            result++;
    }

    PG_RETURN_INT32(result);
}
```