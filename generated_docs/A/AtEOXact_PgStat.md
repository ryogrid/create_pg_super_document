# AtEOXact_PgStat

## Location
[src/backend/utils/activity/pgstat_xact.c:40-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_xact.c#L40-L66)

## Overview
Handles the end-of-transaction cleanup for PostgreSQL statistics, called during top-level transaction commit or abort to process accumulated statistics data.

## Definition
```c
void AtEOXact_PgStat(bool isCommit, bool parallel)
```

## Detailed Description
AtEOXact_PgStat is a critical function in PostgreSQL's statistics subsystem that performs cleanup and finalization tasks when a top-level transaction completes. It coordinates the handling of various types of statistics that were accumulated during the transaction lifetime. The function processes both database-level statistics and transactional statistics information, ensuring proper cleanup regardless of whether the transaction commits or aborts. It also manages the statistics snapshot state to maintain consistency across transaction boundaries.

## Parameters / Member Variables
- `isCommit`: Boolean indicating whether the transaction is committing (true) or aborting (false)
- `parallel`: Boolean indicating whether this is being called in the context of a parallel transaction

## Dependencies
- Functions called/Symbols referenced:
  - [AtEOXact_PgStat_Database](AtEOXact_PgStat_Database.md)
  - [AtEOXact_PgStat_Relations](AtEOXact_PgStat_Relations.md)  
  - [AtEOXact_PgStat_DroppedStats](AtEOXact_PgStat_DroppedStats.md)
  - [pgstat_clear_snapshot](../p/pgstat_clear_snapshot.md)
  - [PgStat_SubXactStatus](../P/PgStat_SubXactStatus.md) (struct type)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md) (src/backend/access/transam/xact.c:2420)
  - [AbortTransaction](AbortTransaction.md) (src/backend/access/transam/xact.c:2929)
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md) (src/backend/access/transam/twophase.c:1661)

## Notes and Other Information
- This function is called from access/transam/xact.c at top-level transaction commit/abort only
- It processes the pgStatXactStack which maintains nested transaction state information
- The function ensures that any existing statistics snapshot is cleared after transaction completion
- It handles both commit and abort scenarios, with different processing logic for each case
- The function validates that it's being called at the correct nesting level (nest_level == 1) for top-level transactions