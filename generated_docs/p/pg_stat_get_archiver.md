# pg_stat_get_archiver

## Location
[src/backend/utils/adt/pgstatfuncs.c:1830-1895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1830-L1895)

## Overview
This function retrieves comprehensive statistical information about PostgreSQL's WAL archiver process, returning data as a structured tuple with details about archiving activity and failures.

## Definition


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
- : Standard PostgreSQL function argument macro (no specific arguments for this function)

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