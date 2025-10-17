# AllTablesyncsReady

## Location
[src/backend/replication/logical/tablesync.c:1757-1781](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/tablesync.c#L1757-L1781)

## Overview
AllTablesyncsReady checks whether all tables in a subscription have completed their initial synchronization and are in the READY state. It returns false if the subscription has no tables or if any table synchronization is still pending.

## Definition
```c
bool AllTablesyncsReady(void)
```

## Detailed Description
This function determines if all table synchronizations for the current subscription are complete and ready for normal replication. It fetches the current state of all subscription tables and checks if they have all reached the READY state, indicating that initial table synchronization has completed successfully.

The function performs several key operations: it fetches up-to-date synchronization state information for all subscription tables, commits any transaction that was started during the fetch process, and then evaluates whether all tables are ready. The function uses the global variable `table_states_not_ready` to determine if any tables are still pending synchronization.

This function is specifically designed to be called from within apply or tablesync workers where MySubscription has already been initialized, and should not be called from other contexts.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [FetchTableStates](../F/FetchTableStates.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [pgstat_report_stat](../p/pgstat_report_stat.md)
- Called from (representative examples):
  - [pa_can_start](../p/pa_can_start.md)
  - [tablesync_start_time_mapping](../t/tablesync_start_time_mapping.md)
  - [run_apply_worker](../r/run_apply_worker.md)

## Notes and Other Information
- Located in src/backend/replication/logical/tablesync.c:1757-1781
- Returns false if subscription has no tables or if any table is not in READY state
- Returns true only when subscription has tables and all are READY
- Requires MySubscription to be initialized (not suitable for external callers)
- May start and commit a transaction internally to fetch current table states
- Uses the global list `table_states_not_ready` (NIL when all tables are ready)
- Part of PostgreSQL's logical replication table synchronization infrastructure

## Simplified Source

```c
bool AllTablesyncsReady(void) {
    bool started_tx = false;
    bool has_subrels = false;

    // Fetch current synchronization state for all subscription tables
    has_subrels = FetchTableStates(&started_tx);

    // Clean up transaction if one was started
    if (started_tx) {
        CommitTransactionCommand();
        pgstat_report_stat(true);
    }

    // Return true only if subscription has tables AND all are ready
    return has_subrels && (table_states_not_ready == NIL);
}
```