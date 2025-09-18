# create_new_objects

## Location
[src/bin/pg_upgrade/pg_upgrade.c:536-659](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.c#L536-L659)

## Overview
Restores database schemas in the new PostgreSQL cluster during pg_upgrade by executing pg_restore on database dump files, handling template1 separately and supporting parallel restoration for better performance.

## Definition


## Detailed Description
This function is responsible for restoring all database schemas and objects from the old cluster to the new cluster during the pg_upgrade process. It operates in several phases:

1. **Template1 Processing**: Processes the template1 database first in a separate, non-parallelized pass. This is necessary because template1 cannot be processed concurrently with other databases since connection attempts would fail when it's transiently dropped during restoration.

2. **Parallel Database Processing**: Processes all other databases (except template1) using parallel execution when enabled. The function uses  to run multiple pg_restore commands simultaneously.

3. **Transaction Size Management**: Adjusts the pg_restore transaction size based on the number of parallel jobs to prevent exceeding lock limits. When running multiple jobs in parallel, it divides the standard transaction size by the number of jobs.

4. **Special Database Handling**: 
   - template1: Uses  options since it already exists
   - postgres: Uses  options since it already exists  
   - Other databases: Uses  option only

5. **Post-restoration Tasks**: After all databases are restored, it handles version-specific tasks like setting frozen XIDs for pre-9.3 clusters and updates cluster information.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status_progress](../p/prep_status_progress.md): Shows progress status to user
  - [pg_log](../p/pg_log.md): Logs status messages
  - [exec_prog](../e/exec_prog.md): Executes pg_restore for template1
  - [parallel_exec_prog](../p/parallel_exec_prog.md): Executes pg_restore in parallel for other databases
  - [cluster_conn_opts](cluster_conn_opts.md): Generates connection options
  - [reap_child](../r/reap_child.md): Waits for parallel child processes to complete
  - [end_progress_output](../e/end_progress_output.md): Ends progress display
  - [check_ok](check_ok.md): Verifies operations completed successfully
  - [set_frozenxids](../s/set_frozenxids.md): Sets frozen XIDs for pre-9.3 clusters
  - [get_db_rel_and_slot_infos](../g/get_db_rel_and_slot_infos.md): Updates cluster information after restoration
- Data structures used:
  - [DbInfo](../D/DbInfo.md): Database information structure
  - old_cluster.dbarr: Array of databases in old cluster
  - new_cluster: New cluster connection information
- Constants used:
  - DB_DUMP_FILE_MASK: Template for database dump filenames
  - DB_DUMP_LOG_FILE_MASK: Template for log filenames
  - RESTORE_TRANSACTION_SIZE: Default transaction size for pg_restore
  - PG_STATUS: Log level for status messages
- Called from:
  - [main](../m/main.md): Part of the main pg_upgrade workflow

## Notes and Other Information
- The function handles both serial and parallel execution modes based on user_opts.jobs
- Template1 must be processed separately because it cannot be dropped while other databases are being created
- Transaction size is automatically adjusted in parallel mode to prevent lock table overflow
- The function includes special handling for pre-9.3 clusters that lack minmxids
- Error handling ensures that if any database restoration fails, the entire upgrade process is aborted
- The function updates the new cluster's metadata after successful restoration to reflect the newly created objects
- Progress reporting keeps users informed during the potentially lengthy restoration process