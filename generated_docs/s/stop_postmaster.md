# stop_postmaster

## Location
[src/bin/pg_upgrade/server.c:331-357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/server.c#L331-L357)

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

## Simplified Source

```c
void stop_postmaster(bool in_atexit) {
    ClusterInfo *cluster;

    // Determine which cluster is currently running
    if (os_info.running_cluster == &old_cluster)
        cluster = &old_cluster;
    else if (os_info.running_cluster == &new_cluster)
        cluster = &new_cluster;
    else
        return;  // No cluster running

    // Execute pg_ctl stop command with appropriate shutdown mode
    // Fast mode for atexit cleanup, smart mode for normal stops
    exec_prog(SERVER_STOP_LOG_FILE, NULL, !in_atexit, !in_atexit,
              "\"%s/pg_ctl\" -w -D \"%s\" -o \"%s\" %s stop",
              cluster->bindir, cluster->pgconfig,
              cluster->pgopts ? cluster->pgopts : "",
              in_atexit ? "-m fast" : "-m smart");

    // Clear running cluster indicator
    os_info.running_cluster = NULL;
}
```