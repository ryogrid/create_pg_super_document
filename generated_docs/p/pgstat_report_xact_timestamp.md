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
  - [PgBackendStatus](../P/PgBackendStatus.md) (structure type)
  - PGSTAT_BEGIN_WRITE_ACTIVITY (macro for atomic write protocol)
  - PGSTAT_END_WRITE_ACTIVITY (macro for atomic write protocol)
- Called from:
  - [StartTransaction](../S/StartTransaction.md) (at transaction start)
  - [CommitTransaction](../C/CommitTransaction.md) (clears timestamp on commit)
  - [PrepareTransaction](../P/PrepareTransaction.md) (in prepared transaction scenarios)
  - [AbortTransaction](../A/AbortTransaction.md) (clears timestamp on abort)

## Notes and Other Information
- Uses volatile pointer semantics to prevent compiler optimizations that could break the atomic update protocol
- Part of PostgreSQL's backend activity monitoring system
- Critical for transaction duration tracking and deadlock detection
- The st_changecount protocol ensures readers can detect when the status is being updated

## Simplified Source

```c
// Simplified version of pgstat_report_xact_timestamp
void pgstat_report_xact_timestamp(TimestampTz tstamp) {
    // Get reference to this backend's status entry
    volatile PgBackendStatus *beentry = MyBEEntry;

    // Only proceed if activity tracking is enabled and backend entry exists
    if (!pgstat_track_activities || !beentry)
        return;

    // Atomically update transaction start timestamp
    // Begin atomic write operation
    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

    // Set the transaction start timestamp (zero means no active transaction)
    beentry->st_xact_start_timestamp = tstamp;

    // End atomic write operation
    PGSTAT_END_WRITE_ACTIVITY(beentry);
}
```

Key simplifications made:
- Added descriptive comments explaining each logical step
- Simplified the volatile pointer explanation to focus on core purpose
- Clarified the atomic write protocol with inline comments
- Emphasized the zero timestamp meaning (no active transaction)
- Removed detailed technical commentary about compiler optimizations