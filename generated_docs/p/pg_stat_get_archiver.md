# pg_stat_get_archiver

## Location
[src/backend/utils/adt/pgstatfuncs.c:1830-1895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1830-L1895)

## Overview
This function retrieves comprehensive statistical information about PostgreSQL's WAL archiver process, returning data as a structured tuple with details about archiving activity and failures.

## Definition

```c
Datum
pg_stat_get_archiver(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function provides detailed statistics about PostgreSQL's Write-Ahead Log (WAL) archiver process. It creates and returns a tuple containing seven fields of archiver-related metrics:

1. **archived_count**: Number of WAL files successfully archived
2. **last_archived_wal**: Name of the last successfully archived WAL file
3. **last_archived_time**: Timestamp of the last successful archival
4. **failed_count**: Number of failed archival attempts
5. **last_failed_wal**: Name of the WAL file that last failed to archive
6. **last_failed_time**: Timestamp of the last archival failure
7. **stats_reset**: Timestamp when archiver statistics were last reset

The function handles NULL values appropriately for optional fields (empty strings for WAL names and zero timestamps are converted to SQL NULL values). It fetches the current archiver statistics using  and formats them into a PostgreSQL tuple for SQL consumption.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro (no specific arguments for this function)
## Dependencies
- Functions called/Symbols referenced:
  -  - Retrieves current archiver statistics
  -  - Creates tuple descriptor with specified number of attributes
  -  - Initializes individual tuple descriptor entries
  -  - Finalizes tuple descriptor for use
  -  - Converts int64 values to PostgreSQL Datum
  -  - Converts C strings to PostgreSQL text Datum
  -  - Converts timestamps to PostgreSQL timestamptz Datum
  -  - Creates heap tuple from values and nulls arrays
  -  - Converts heap tuple to Datum
  -  - Returns Datum from PostgreSQL function

- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL interface)

## Notes and Other Information
- This function is exposed as a SQL function, typically accessed through system views like 
- Part of PostgreSQL's comprehensive statistics collection system for monitoring WAL archiving performance
- Located in 
- The function properly handles edge cases by converting empty strings and zero timestamps to SQL NULL values
- Essential for monitoring archival health in PostgreSQL installations with WAL archiving enabled
- Returns a composite type with seven attributes, making it suitable for use in SQL queries and monitoring applications
- The archiver statistics help database administrators monitor backup and replication processes

## Simplified Source

```c
Datum
pg_stat_get_archiver(PG_FUNCTION_ARGS)
{
    TupleDesc tupdesc;
    Datum values[7] = {0};
    bool nulls[7] = {0};
    PgStat_ArchiverStats *archiver_stats;

    // Create tuple descriptor for 7 archiver statistics fields
    tupdesc = CreateTemplateTupleDesc(7);
    TupleDescInitEntry(tupdesc, 1, "archived_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 2, "last_archived_wal", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, 3, "last_archived_time", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, 4, "failed_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 5, "last_failed_wal", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, 6, "last_failed_time", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, 7, "stats_reset", TIMESTAMPTZOID, -1, 0);
    BlessTupleDesc(tupdesc);

    // Fetch current archiver statistics
    archiver_stats = pgstat_fetch_stat_archiver();

    // Fill values array, handling NULLs for empty/zero values
    values[0] = Int64GetDatum(archiver_stats->archived_count);

    if (*(archiver_stats->last_archived_wal) == '\0')
        nulls[1] = true;
    else
        values[1] = CStringGetTextDatum(archiver_stats->last_archived_wal);

    if (archiver_stats->last_archived_timestamp == 0)
        nulls[2] = true;
    else
        values[2] = TimestampTzGetDatum(archiver_stats->last_archived_timestamp);

    values[3] = Int64GetDatum(archiver_stats->failed_count);

    if (*(archiver_stats->last_failed_wal) == '\0')
        nulls[4] = true;
    else
        values[4] = CStringGetTextDatum(archiver_stats->last_failed_wal);

    if (archiver_stats->last_failed_timestamp == 0)
        nulls[5] = true;
    else
        values[5] = TimestampTzGetDatum(archiver_stats->last_failed_timestamp);

    if (archiver_stats->stat_reset_timestamp == 0)
        nulls[6] = true;
    else
        values[6] = TimestampTzGetDatum(archiver_stats->stat_reset_timestamp);

    // Return tuple as Datum
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
```