# copy_xact_xlog_xid

## Location
src/bin/pg_upgrade/pg_upgrade.c: 702 - 826

## Overview
Copies transaction commit logs and multixact data from the old cluster to the new cluster, then resets transaction IDs, multixact IDs, and WAL archives to maintain transaction continuity during pg_upgrade.

## Definition


## Detailed Description
This function is responsible for preserving transaction state and continuity between the old and new PostgreSQL clusters during an upgrade. It performs several critical operations:

1. **Transaction Log Copy**: Copies commit logs from the old cluster, handling the naming change from pg_clog (pre-v10) to pg_xact (v10+) based on the cluster versions.

2. **Transaction ID Management**: Uses pg_resetwal to set:
   - Oldest XID in the new cluster
   - Next transaction ID and epoch
   - Commit timestamp limits

3. **Multixact Handling**: Conditionally handles multixact data based on format compatibility:
   - **Compatible versions**: Copies pg_multixact/offsets and pg_multixact/members directories and preserves multixact counters
   - **Incompatible versions**: Removes existing multixact offsets and sets appropriate multixact boundaries to prevent reading obsolete data

4. **WAL Archive Reset**: Resets WAL archives in the new cluster using timeline 1 and the appropriate log file sequence from the old cluster.

The function includes extensive error checking and user status updates throughout the process.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - GET_MAJOR_VERSION: Determines PostgreSQL major version for compatibility checks
  - [copy_subdir_files](copy_subdir_files.md): Copies entire subdirectories between clusters
  - [prep_status](../p/prep_status.md): Displays operation status to user
  - [exec_prog](../e/exec_prog.md): Executes pg_resetwal commands with various options
  - [check_ok](check_ok.md): Verifies each operation completed successfully
  - [remove_new_subdir](../r/remove_new_subdir.md): Removes incompatible multixact offset files
- Constants used:
  - UTILITY_LOG_FILE: Log file for utility operations
  - MULTIXACT_FORMATCHANGE_CAT_VER: Version constant for multixact format compatibility
- Data structures used:
  - old_cluster.controldata: Contains transaction and multixact state from old cluster
  - new_cluster: Contains paths and connection info for new cluster
- Called from:
  - [main](../m/main.md): Part of the main pg_upgrade workflow after schema restoration

## Notes and Other Information
- The function handles version-specific directory naming (pg_clog vs pg_xact) automatically
- Multixact handling varies significantly based on format compatibility between cluster versions
- All pg_resetwal operations use the -f flag to force execution without prompts
- The function preserves transaction continuity to prevent data corruption or visibility issues
- WAL archives are reset to timeline 1 with no history file to match a fresh cluster state
- Error handling ensures that any failure in transaction state copying will abort the upgrade
- The commit timestamp limits are set to prevent issues with commit timestamp tracking
- Multixact ID handling prevents the new cluster from attempting to read obsolete multixact data
- The function is critical for maintaining ACID properties across the upgrade process