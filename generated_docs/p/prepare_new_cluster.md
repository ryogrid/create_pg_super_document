# prepare_new_cluster

## Location
[src/bin/pg_upgrade/pg_upgrade.c:484-513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.c#L484-L513)

## Overview
Prepares the new cluster for data migration by analyzing and freezing all rows, ensuring optimal performance and transaction ID management after pg_upgrade completion.

## Definition

```c
static void
prepare_new_cluster(void)
```
## Detailed Description
The prepare_new_cluster function performs critical post-schema-loading optimization of the new cluster. It executes two essential maintenance operations:

1. **Analyze Phase**: Runs ANALYZE on all databases to generate statistics for the query planner. This ensures optimal query performance immediately after the upgrade without waiting for autovacuum to collect statistics later.

2. **Freeze Phase**: Performs VACUUM FREEZE on all databases to freeze transaction IDs in data rows. This prevents transaction ID wraparound issues and establishes a clean baseline for the new cluster's transaction ID management.

The function carefully sequences these operations - analyze before freeze - to preserve frozenxids that were restored during schema loading while ensuring pg_statistic tables are also properly frozen.

## Parameters / Member Variables
No parameters - operates on the global new_cluster structure.

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](prep_status.md) (status reporting)
  - [exec_prog](../e/exec_prog.md) (external program execution)
  - [cluster_conn_opts](../c/cluster_conn_opts.md) (connection options formatting)
  - [check_ok](../c/check_ok.md) (status verification)
  - UTILITY_LOG_FILE (log file constant)
- Called from:
  - [main](../m/main.md) (from pg_upgrade.c:153)

## Notes and Other Information
- Critical for post-upgrade performance and transaction ID management
- Uses vacuumdb utility with --all --analyze and --all --freeze options
- Template0 special handling: data rows frozen by initdb, metadata updated later
- Prevents autovacuum from updating statistics by pre-generating them
- Essential for maintaining frozenxids restored during schema loading
- Part of the final preparation phase before completing the upgrade process
- Ensures the new cluster is immediately ready for production use