# create_new_objects

## Location
[src/bin/pg_upgrade/pg_upgrade.c:536-659](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.c#L536-L659)

## Overview
Restores database schemas in the new PostgreSQL cluster during pg_upgrade by executing pg_restore on database dump files, handling template1 separately and supporting parallel restoration for better performance.

## Definition

```c
static void
create_new_objects(void)
```
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

## Simplified Source

```c
static void create_new_objects(void) {
    int dbnum;

    prep_status_progress("Restoring database schemas in the new cluster");

    // Phase 1: Process template1 database separately (not parallelized)
    // template1 cannot be processed concurrently because connection attempts
    // would fail when it's transiently dropped
    for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++) {
        char sql_file_name[MAXPGPATH], log_file_name[MAXPGPATH];
        DbInfo *old_db = &old_cluster.dbarr.dbs[dbnum];

        if (strcmp(old_db->db_name, "template1") != 0)
            continue;

        pg_log(PG_STATUS, "%s", old_db->db_name);
        snprintf(sql_file_name, sizeof(sql_file_name), DB_DUMP_FILE_MASK, old_db->db_oid);
        snprintf(log_file_name, sizeof(log_file_name), DB_DUMP_LOG_FILE_MASK, old_db->db_oid);

        // template1 already exists in target, so use --clean --create
        exec_prog(log_file_name, NULL, true, true,
                 "\"%s/pg_restore\" %s --clean --create --exit-on-error --verbose "
                 "--transaction-size=%d --dbname postgres \"%s/%s\"",
                 new_cluster.bindir,
                 cluster_conn_opts(&new_cluster),
                 RESTORE_TRANSACTION_SIZE,
                 log_opts.dumpdir,
                 sql_file_name);
        break; // Done once we've processed template1
    }

    // Phase 2: Process all other databases in parallel
    for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++) {
        char sql_file_name[MAXPGPATH], log_file_name[MAXPGPATH];
        DbInfo *old_db = &old_cluster.dbarr.dbs[dbnum];
        const char *create_opts;
        int txn_size;

        if (strcmp(old_db->db_name, "template1") == 0)
            continue; // Skip template1 in this pass

        pg_log(PG_STATUS, "%s", old_db->db_name);
        snprintf(sql_file_name, sizeof(sql_file_name), DB_DUMP_FILE_MASK, old_db->db_oid);
        snprintf(log_file_name, sizeof(log_file_name), DB_DUMP_LOG_FILE_MASK, old_db->db_oid);

        // Special handling for postgres database
        if (strcmp(old_db->db_name, "postgres") == 0)
            create_opts = "--clean --create";  // postgres already exists
        else
            create_opts = "--create";          // new database

        // Adjust transaction size for parallel execution
        txn_size = RESTORE_TRANSACTION_SIZE;
        if (user_opts.jobs > 1) {
            txn_size /= user_opts.jobs;
            txn_size = Max(txn_size, 10);  // Maintain minimum sanity
        }

        // Execute pg_restore in parallel
        parallel_exec_prog(log_file_name, NULL,
                          "\"%s/pg_restore\" %s %s --exit-on-error --verbose "
                          "--transaction-size=%d --dbname template1 \"%s/%s\"",
                          new_cluster.bindir,
                          cluster_conn_opts(&new_cluster),
                          create_opts,
                          txn_size,
                          log_opts.dumpdir,
                          sql_file_name);
    }

    // Wait for all parallel jobs to complete
    while (reap_child(true) == true)
        ;

    end_progress_output();
    check_ok();

    // Handle pre-9.3 cluster compatibility: set frozen XIDs
    if (GET_MAJOR_VERSION(old_cluster.major_version) <= 902)
        set_frozenxids(true);

    // Update cluster information now that we have objects in databases
    get_db_rel_and_slot_infos(&new_cluster, false);
}
```