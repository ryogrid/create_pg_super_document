# prepare_new_globals

## Location
[src/bin/pg_upgrade/pg_upgrade.c:514-535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.c#L514-L535)

## Overview
Sets up global objects in the new cluster during pg_upgrade by restoring frozen XIDs for initdb-created tables and restoring global database objects like roles and tablespaces.

## Definition

```c
static void
prepare_new_globals(void)
```
## Detailed Description
This function is a critical step in the pg_upgrade process that prepares the global objects in the new PostgreSQL cluster. It performs two main operations in sequence:

1. First, it calls  to establish the proper frozen transaction IDs for tables created during initdb. This ensures transaction visibility is correctly maintained for system tables in the upgraded cluster.

2. Second, it restores global database objects (roles and tablespaces) by executing psql to load the globals dump file that was created from the old cluster. The restoration uses the GLOBALS_DUMP_FILE which contains SQL commands to recreate all global objects.

The function provides user feedback through  and uses  to run psql with appropriate connection options and the globals dump file.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [set_frozenxids](../s/set_frozenxids.md): Sets frozen XIDs for initdb-created tables
  - [prep_status](prep_status.md): Displays status message to user
  - [exec_prog](../e/exec_prog.md): Executes external programs (psql in this case)
  - [cluster_conn_opts](../c/cluster_conn_opts.md): Generates connection options for the cluster
  - [check_ok](../c/check_ok.md): Verifies the previous operation completed successfully
- Constants used:
  - UTILITY_LOG_FILE: Log file for utility operations
  - EXEC_PSQL_ARGS: Standard arguments for psql execution
  - GLOBALS_DUMP_FILE: Filename containing dumped global objects
- Called from:
  - [main](../m/main.md): Part of the main pg_upgrade workflow

## Notes and Other Information
- This function is called during the restoration phase of pg_upgrade after the schema has been created
- The order of operations is important: frozen XIDs must be set before restoring global objects
- The function assumes that the globals dump file has already been created in a previous step
- Error handling is performed through check_ok() which will terminate the upgrade if the psql command fails
- The function operates on the new_cluster global variable which contains connection and directory information

## Simplified Source

```c
static void prepare_new_globals(void) {
    // Set frozen XIDs for initdb-created tables first
    // This ensures proper transaction visibility for system tables
    set_frozenxids(false);

    // Restore global objects (roles and tablespaces) from dump file
    prep_status("Restoring global objects in the new cluster");

    exec_prog(UTILITY_LOG_FILE, NULL, true, true,
              "\"%s/psql\" " EXEC_PSQL_ARGS " %s -f \"%s/%s\"",
              new_cluster.bindir,
              cluster_conn_opts(&new_cluster),
              log_opts.dumpdir,
              GLOBALS_DUMP_FILE);

    check_ok();
}
```