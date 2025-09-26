# pgstat_clear_snapshot

## Location
[src/backend/utils/activity/pgstat.c:781-810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L781-L810)

## Overview
This function discards any statistics data collected in the current transaction and clears the statistics snapshot, forcing subsequent requests to read fresh snapshots from the statistics system.

## Definition

```c
struct PgStat_HashKey));
```
## Detailed Description
The  function provides a mechanism to invalidate and clear the current statistics snapshot held by the local backend. This function is essential for maintaining consistency in PostgreSQL's statistics system, particularly when transaction boundaries are crossed or when configuration changes require fresh statistics data.

The function performs several cleanup operations: it resets the validity flags for fixed statistics, clears the snapshot statistics pointer, resets the fetch consistency mode, and releases any memory context allocated for the snapshot. Additionally, it forwards the cleanup request to the backend activity snapshot system for historical compatibility reasons.

This function is automatically invoked during transaction commit or abort to ensure that stale snapshot data doesn't persist across transaction boundaries. It can also be triggered by changes to the  configuration parameter.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_assert_is_up](pgstat_assert_is_up.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [pgstat_clear_backend_activity_snapshot](pgstat_clear_backend_activity_snapshot.md)
  - PGSTAT_FETCH_CONSISTENCY_NONE (constant)
- Called from (representative examples):
  - [pgstat_get_stat_snapshot_timestamp](pgstat_get_stat_snapshot_timestamp.md)
  - [pgstat_snapshot_fixed](pgstat_snapshot_fixed.md)
  - [pgstat_prep_snapshot](pgstat_prep_snapshot.md)
  - [AtEOXact_PgStat](../A/AtEOXact_PgStat.md)
  - [PostPrepare_PgStat](../P/PostPrepare_PgStat.md)
  - [pg_stat_clear_snapshot](pg_stat_clear_snapshot.md)

## Notes and Other Information
- This function is automatically called during transaction commit or abort to discard no-longer-wanted snapshots
- Changes to the  configuration can trigger this function to be called
- The function maintains historical compatibility by forwarding reset requests to the backend activity snapshot system
- Memory allocated for the snapshot context is properly freed to prevent memory leaks
- The  flag is reset at the end of the function to handle forced cleanup scenarios
- This is a critical function for ensuring statistics consistency across transaction boundaries