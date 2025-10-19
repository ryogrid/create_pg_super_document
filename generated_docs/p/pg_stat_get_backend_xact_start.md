# pg_stat_get_backend_xact_start

## Location
[src/backend/utils/adt/pgstatfuncs.c:835-856](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L835-L856)

## Overview
Returns the timestamp when the current transaction started for a specific backend process.

## Definition
```c
Datum pg_stat_get_backend_xact_start(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the start timestamp of the currently active transaction for a backend process identified by its process number. It accesses the backend status entry and returns the st_xact_start_timestamp field, which records when the current transaction began. The function returns NULL when backend information is unavailable, when the user lacks sufficient privileges, or when the backend is not currently in a transaction.

## Parameters / Member Variables
- `procNumber` (int32): The process number identifying the target backend process

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_beentry_by_proc_number](pgstat_get_beentry_by_proc_number.md)
  - HAS_PGSTAT_PERMISSIONS
  - PG_RETURN_TIMESTAMPTZ
- Data types used:
  - [PgBackendStatus](../P/PgBackendStatus.md)
  - TimestampTz

## Notes and Other Information
- Returns NULL if backend information is not available
- Returns NULL if the user has insufficient privileges to view the backend information
- Returns NULL if the backend is not currently in a transaction (st_xact_start_timestamp == 0)
- The timestamp represents when the current transaction started, which may be different from when the current query started
- Used by pg_stat_activity view to show transaction start times
- Helps identify long-running transactions that may be holding locks or causing performance issues
- A transaction can span multiple queries, so this timestamp may be older than the activity start timestamp

## Simplified Source

```c
Datum
pg_stat_get_backend_xact_start(PG_FUNCTION_ARGS)
{
    int32 procNumber = PG_GETARG_INT32(0);
    TimestampTz result;
    PgBackendStatus *beentry;

    // Get backend entry by process number
    if ((beentry = pgstat_get_beentry_by_proc_number(procNumber)) == NULL)
        PG_RETURN_NULL();

    // Check user permissions
    if (!HAS_PGSTAT_PERMISSIONS(beentry->st_userid))
        PG_RETURN_NULL();

    result = beentry->st_xact_start_timestamp;

    // Return NULL if not in a transaction
    if (result == 0)
        PG_RETURN_NULL();

    PG_RETURN_TIMESTAMPTZ(result);
}
```