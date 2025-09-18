# stop_postmaster

## Location
src/bin/pg_upgrade/server.c: 331 - 357

## Overview
Stops the currently running PostgreSQL postmaster process during pg_upgrade operations, using either fast or smart shutdown modes.

## Definition
```c
void stop_postmaster(bool in_atexit)
```

## Detailed Description
This function gracefully stops the PostgreSQL postmaster process that was started by pg_upgrade. It determines which cluster (old or new) is currently running and constructs the appropriate pg_ctl stop command. The function chooses between fast shutdown mode (when called during exit cleanup) and smart shutdown mode (for normal stops). After successfully stopping the server, it clears the running cluster reference to indicate no server is active.

## Parameters / Member Variables
- `in_atexit`: Boolean flag indicating if called from an atexit handler, which affects shutdown mode selection

## Dependencies
- Functions called/Symbols referenced:
  - [exec_prog](../e/exec_prog.md)
  - SERVER_STOP_LOG_FILE (constant)
- Called from (representative examples):
  - [stop_postmaster_atexit](stop_postmaster_atexit.md)
  - [check_and_dump_old_cluster](../c/check_and_dump_old_cluster.md)
  - [report_clusters_compatible](../r/report_clusters_compatible.md)
  - [issue_warnings_and_set_wal_level](../i/issue_warnings_and_set_wal_level.md)
  - [main](../m/main.md) (multiple locations in pg_upgrade.c)
  - [setup](setup.md)

## Notes and Other Information
- Uses fast shutdown (-m fast) when called from atexit handler for quicker cleanup
- Uses smart shutdown (-m smart) for normal operations to allow graceful connection termination
- Automatically detects which cluster is running (old_cluster vs new_cluster)
- Clears os_info.running_cluster to NULL after successful shutdown
- Returns immediately if no cluster is currently running
- Located in src/bin/pg_upgrade/server.c:331-357