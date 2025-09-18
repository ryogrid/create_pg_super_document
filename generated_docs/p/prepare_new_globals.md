# prepare_new_globals

## Location
src/bin/pg_upgrade/pg_upgrade.c: 514 - 535

## Overview
Sets up global objects in the new cluster during pg_upgrade by restoring frozen XIDs for initdb-created tables and restoring global database objects like roles and tablespaces.

## Definition


## Detailed Description
This function is a critical step in the pg_upgrade process that prepares the global objects in the new PostgreSQL cluster. It performs two main operations in sequence:

1. First, it calls  to establish the proper frozen transaction IDs for tables created during initdb. This ensures transaction visibility is correctly maintained for system tables in the upgraded cluster.

2. Second, it restores global database objects (roles and tablespaces) by executing psql to load the globals dump file that was created from the old cluster. The restoration uses the GLOBALS_DUMP_FILE which contains SQL commands to recreate all global objects.

The function provides user feedback through  and uses  to run psql with appropriate connection options and the globals dump file.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - set_frozenxids: Sets frozen XIDs for initdb-created tables
  - prep_status: Displays status message to user
  - exec_prog: Executes external programs (psql in this case)
  - cluster_conn_opts: Generates connection options for the cluster
  - check_ok: Verifies the previous operation completed successfully
- Constants used:
  - UTILITY_LOG_FILE: Log file for utility operations
  - EXEC_PSQL_ARGS: Standard arguments for psql execution
  - GLOBALS_DUMP_FILE: Filename containing dumped global objects
- Called from:
  - main: Part of the main pg_upgrade workflow

## Notes and Other Information
- This function is called during the restoration phase of pg_upgrade after the schema has been created
- The order of operations is important: frozen XIDs must be set before restoring global objects
- The function assumes that the globals dump file has already been created in a previous step
- Error handling is performed through check_ok() which will terminate the upgrade if the psql command fails
- The function operates on the new_cluster global variable which contains connection and directory information