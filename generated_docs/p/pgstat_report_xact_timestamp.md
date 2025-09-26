# pgstat_report_xact_timestamp

## Location
[src/backend/utils/activity/backend_status.c:682-708](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L682-L708)

## Overview
Reports the current transaction start timestamp to the backend status subsystem for activity monitoring and statistics collection.

## Definition
```c
void pgstat_report_xact_timestamp(TimestampTz tstamp)
```

## Detailed Description
This function updates the current backend's transaction start timestamp in the shared backend status structure. It is used to track when transactions begin and end, providing visibility into transaction activity for monitoring purposes. The function follows PostgreSQL's standard protocol for updating shared status information by using atomic operations to prevent race conditions when multiple processes might be reading the status concurrently.

When called with a zero timestamp, it indicates there is no active transaction. The function only operates if activity tracking is enabled (`pgstat_track_activities`) and the backend entry exists.

## Parameters / Member Variables
- `tstamp`: The transaction start timestamp to record. A value of zero indicates no active transaction.

## Dependencies
- Functions called/Symbols referenced:
  - PgBackendStatus (structure type)
  - PGSTAT_BEGIN_WRITE_ACTIVITY (macro for atomic write protocol)
  - PGSTAT_END_WRITE_ACTIVITY (macro for atomic write protocol)
- Called from:
  - StartTransaction (at transaction start)
  - CommitTransaction (clears timestamp on commit)
  - PrepareTransaction (in prepared transaction scenarios)
  - AbortTransaction (clears timestamp on abort)

## Notes and Other Information
- Uses volatile pointer semantics to prevent compiler optimizations that could break the atomic update protocol
- Part of PostgreSQL's backend activity monitoring system
- Critical for transaction duration tracking and deadlock detection
- The st_changecount protocol ensures readers can detect when the status is being updated