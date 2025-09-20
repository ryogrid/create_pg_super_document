# pgstat_report_checksum_failure

## Location
[src/backend/utils/activity/pgstat_database.c:166-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_database.c#L166-L174)

## Overview
Reports a single checksum failure in the current database by incrementing the database-level checksum failure counter in PostgreSQL's statistics system.

## Definition

```c
void
pgstat_report_checksum_failure(void)
```
## Detailed Description
This function serves as a convenience wrapper that reports exactly one checksum failure in the current database (identified by MyDatabaseId). It is typically called when a page checksum verification fails during buffer page operations. The function delegates the actual statistics update to pgstat_report_checksum_failures_in_db(), passing the current database ID and a failure count of 1.

The function operates by updating shared statistics directly, which is acceptable because checksum failures are expected to be rare events that won't cause performance issues from frequent shared memory updates.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_report_checksum_failures_in_db](pgstat_report_checksum_failures_in_db.md)
  - MyDatabaseId (global variable)
- Called from (representative examples):
  - PageIsVerifiedExtended (in src/backend/storage/page/bufpage.c:156)

## Notes and Other Information
- This function only operates when pgstat_track_counts is enabled
- Checksum failures are considered rare events, so direct shared memory updates are used rather than buffering
- The function updates both the failure count and the timestamp of the last checksum failure
- This is part of PostgreSQL's database-level statistics collection system for monitoring data integrity