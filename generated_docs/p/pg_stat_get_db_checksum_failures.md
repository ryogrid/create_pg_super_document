# pg_stat_get_db_checksum_failures

## Location
[src/backend/utils/adt/pgstatfuncs.c:1112-1129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1112-L1129)

## Overview
Returns the number of data page checksum failures that have been detected in a specific database, indicating potential data corruption.

## Definition
```c
Datum pg_stat_get_db_checksum_failures(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the count of checksum verification failures for a specified database. Data page checksums are a PostgreSQL feature that helps detect data corruption by storing and verifying checksums for each data page. When a page is read from disk, its checksum is recalculated and compared with the stored checksum.

Key aspects of the function:
1. First checks if data checksums are enabled for the cluster using DataChecksumsEnabled()
2. If checksums are disabled, returns NULL since no checksum verification occurs
3. If checksums are enabled, fetches the database statistics entry
4. Returns the checksum_failures counter from the database entry
5. Returns 0 if no database statistics entry exists

Checksum failures indicate potential hardware issues, storage corruption, or other data integrity problems that require immediate attention.

## Parameters / Member Variables
- `dbid` (Oid): The database OID to retrieve checksum failure statistics for

## Dependencies
- Functions called/Symbols referenced:
  - [DataChecksumsEnabled](../D/DataChecksumsEnabled.md)
  - [pgstat_fetch_stat_dbentry](pgstat_fetch_stat_dbentry.md)
- Data types used:
  - [PgStat_StatDBEntry](../P/PgStat_StatDBEntry.md)

## Notes and Other Information
- Returns NULL if data checksums are not enabled for the PostgreSQL cluster
- Checksum failures are serious indicators of potential data corruption
- Data checksums must be enabled when the cluster is initialized (initdb --data-checksums)
- Cannot be enabled or disabled on an existing cluster without full dump/restore
- Used by monitoring systems to detect and alert on data integrity issues
- The counter is cumulative since database startup or statistics reset
- Should be monitored regularly in production environments for early detection of hardware problems
- A non-zero value typically indicates storage or memory hardware issues requiring investigation

## Simplified Source

```c
Datum
pg_stat_get_db_checksum_failures(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry *dbentry;

    // Return NULL if checksums are not enabled
    if (!DataChecksumsEnabled())
        PG_RETURN_NULL();

    // Get database statistics entry
    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = (int64) (dbentry->checksum_failures);

    PG_RETURN_INT64(result);
}
```