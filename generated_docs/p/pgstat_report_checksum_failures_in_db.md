# pgstat_report_checksum_failures_in_db

## Location
src/backend/utils/activity/pgstat_database.c: 140 - 165

## Overview
Records one or more checksum failures for a specific database in PostgreSQL's statistics system.

## Definition
```c
void pgstat_report_checksum_failures_in_db(Oid dboid, int failurecount)
```

## Detailed Description
This function is responsible for tracking checksum validation failures that occur within a specific database. When data corruption is detected through checksum verification, this function updates the database's shared statistics to record both the number of failures and the timestamp of the most recent failure. The function directly updates shared statistics using a locked entry reference, which is acceptable because checksum failures should be rare events. This information is crucial for database administrators to monitor data integrity issues and take appropriate corrective actions.

## Parameters / Member Variables
- `dboid`: The OID (Object Identifier) of the database where checksum failures occurred
- `failurecount`: The number of checksum failures to record (can be multiple failures at once)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_get_entry_ref_locked
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - pgstat_unlock_entry
  - PGSTAT_KIND_DATABASE
  - PgStat_EntryRef
  - [PgStatShared_Database](../P/PgStatShared_Database.md)
- Called from (representative examples):
  - [sendFile](../s/sendFile.md) (in src/backend/backup/basebackup.c:1818)
  - [pgstat_report_checksum_failure](pgstat_report_checksum_failure.md) (in src/backend/utils/activity/pgstat_database.c:168)

## Notes and Other Information
- This function is part of PostgreSQL's data integrity monitoring system
- Located in src/backend/utils/activity/pgstat_database.c:140-165
- Only operates when statistics tracking is enabled (pgstat_track_counts)
- Updates shared statistics directly due to the rarity of checksum failures
- Records both cumulative failure count and timestamp of last failure
- Uses locked access to ensure thread-safe updates to shared statistics
- Essential for monitoring database corruption and data integrity issues