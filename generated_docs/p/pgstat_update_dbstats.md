# pgstat_update_dbstats

## Location
[src/backend/utils/activity/pgstat_database.c:270-323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_database.c#L270-L323)

## Overview
pgstat_update_dbstats is a subroutine that accumulates and reports database-level statistics including transaction counts, I/O timings, and session timing information.

## Definition
void pgstat_update_dbstats(TimestampTz ts)

## Detailed Description
This function serves as a key component of PostgreSQL's statistics reporting system, specifically handling the accumulation of database-level statistics. It updates the database statistics entry with transaction commit/rollback counts, block I/O timing information, and session timing data. The function only operates when connected to a valid database (not shared state). It accumulates counters from global variables into the database's statistics entry and conditionally reports session timing information based on configuration. After updating the statistics, it resets all the global counters to zero for the next reporting cycle.

## Parameters / Member Variables
- : TimestampTz representing the current timestamp for calculating time differences and session timing

## Dependencies
- Functions called/Symbols referenced:
  - [PgStat_StatDBEntry](../P/PgStat_StatDBEntry.md) (statistics entry structure)
  - [pgstat_prep_database_pending](pgstat_prep_database_pending.md) (prepares database statistics entry)
  - [pgstat_should_report_connstat](pgstat_should_report_connstat.md) (checks if connection statistics should be reported)
  - [TimestampDifference](../T/TimestampDifference.md) (calculates time differences)
  - PgStat_Counter (counter type for statistics)
- Called from (representative examples):
  - [pgstat_report_stat](pgstat_report_stat.md) (from src/backend/utils/activity/pgstat.c:646)

## Notes and Other Information
- The function only operates when MyDatabaseId is valid, avoiding attribution of time to shared state (InvalidOid)
- Global counters (pgStatXactCommit, pgStatXactRollback, pgStatBlockReadTime, etc.) are reset to zero after accumulation
- [Session](../S/Session.md) timing is only reported if pgstat_should_report_connstat() returns true
- [Session](../S/Session.md) time is calculated from pgLastSessionReportTime which is initialized by pgstat_report_connect()
- Time values are stored in microseconds for precision
- This function is part of the regular statistics reporting cycle and helps maintain database performance metrics

## Simplified Source

```c
// Simplified version of pgstat_update_dbstats
void
pgstat_update_dbstats(TimestampTz ts)
{
    // Skip if not connected to a valid database
    if (!OidIsValid(MyDatabaseId))
        return;

    // Get database statistics entry
    PgStat_StatDBEntry *dbentry = pgstat_prep_database_pending(MyDatabaseId);

    // Accumulate transaction and I/O statistics
    dbentry->xact_commit += pgStatXactCommit;
    dbentry->xact_rollback += pgStatXactRollback;
    dbentry->blk_read_time += pgStatBlockReadTime;
    dbentry->blk_write_time += pgStatBlockWriteTime;

    // Update session timing if connection stats should be reported
    if (pgstat_should_report_connstat())
    {
        // Calculate time elapsed since last report
        long secs;
        int usecs;
        TimestampDifference(pgLastSessionReportTime, ts, &secs, &usecs);
        pgLastSessionReportTime = ts;

        // Accumulate session timing statistics
        dbentry->session_time += (PgStat_Counter) secs * 1000000 + usecs;
        dbentry->active_time += pgStatActiveTime;
        dbentry->idle_in_transaction_time += pgStatTransactionIdleTime;
    }

    // Reset global counters for next reporting cycle
    pgStatXactCommit = 0;
    pgStatXactRollback = 0;
    pgStatBlockReadTime = 0;
    pgStatBlockWriteTime = 0;
    pgStatActiveTime = 0;
    pgStatTransactionIdleTime = 0;
}
```

Key simplifications made:
- Added step-by-step comments explaining each phase
- Grouped related operations with descriptive comments
- Simplified variable declarations inline
- Preserved essential logic: validate database, accumulate stats, conditionally report session timing, reset counters
- Maintained the precise timing calculations and counter reset behavior