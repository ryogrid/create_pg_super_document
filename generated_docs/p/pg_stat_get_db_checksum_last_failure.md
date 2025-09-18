# pg_stat_get_db_checksum_last_failure

## Location
src/backend/utils/adt/pgstatfuncs.c: 1130 - 1150

## Overview
Returns the timestamp of the last checksum failure that occurred in a specific database.

## Definition
```c
Datum pg_stat_get_db_checksum_last_failure(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the timestamp of the most recent checksum verification failure for a given database. It is part of PostgreSQL's data integrity monitoring system that tracks when checksum validation fails during page reads. The function returns NULL if checksums are not enabled, if the database has no recorded statistics, or if no checksum failures have occurred.

## Parameters / Member Variables
- `dbid`: Database OID (Object Identifier) for which to retrieve the last checksum failure timestamp

## Dependencies
- Functions called/Symbols referenced:
  - [DataChecksumsEnabled](../D/DataChecksumsEnabled.md)
  - [pgstat_fetch_stat_dbentry](pgstat_fetch_stat_dbentry.md)  
  - PG_RETURN_TIMESTAMPTZ
- Called from (representative examples):
  - SQL queries via pg_stat_get_db_checksum_last_failure() function

## Notes and Other Information
- Returns NULL if data checksums are not enabled on the cluster
- Returns NULL if no checksum failures have been recorded for the database
- The timestamp is stored in the PgStat_StatDBEntry structure's last_checksum_failure field
- This function is typically used for monitoring data integrity and diagnosing storage-related issues