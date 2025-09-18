# AtEOXact_PgStat_Database

## Location
src/backend/utils/activity/pgstat_database.c: 249 - 269

## Overview
AtEOXact_PgStat_Database is a function that handles database-level statistics accounting at the end of a transaction (commit or abort).

## Definition
void AtEOXact_PgStat_Database(bool isCommit, bool parallel)

## Detailed Description
This function is called at the end of each transaction to update database-level statistics counters. It tracks the number of committed and aborted transactions for the current database. The function only updates statistics for non-parallel worker transactions to avoid double-counting, as parallel worker statistics are handled separately. The counters (pgStatXactCommit and pgStatXactRollback) are used internally and may not be sent immediately to the statistics collector.

## Parameters / Member Variables
- : Boolean flag indicating whether the transaction was committed (true) or aborted (false)
- : Boolean flag indicating whether this is a parallel worker transaction (true) or a regular transaction (false)

## Dependencies
- Functions called/Symbols referenced:
  - pgStatXactCommit (global counter variable)
  - pgStatXactRollback (global counter variable)
- Called from (representative examples):
  - AtEOXact_PgStat (from src/backend/utils/activity/pgstat_xact.c:44)

## Notes and Other Information
- The function uses counters rather than simple boolean flags because the reporting message to the statistics collector might not be sent immediately
- Parallel worker transactions are excluded from counting to prevent duplicate statistics reporting
- This is part of PostgreSQL's statistics collection system that tracks database activity for performance monitoring and analysis