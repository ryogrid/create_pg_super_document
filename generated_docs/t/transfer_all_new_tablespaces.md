# transfer_all_new_tablespaces

## Location
[src/bin/pg_upgrade/relfilenumber.c:29-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/relfilenumber.c#L29-L88)

## Overview
This function is responsible for transferring all user relation files from the old PostgreSQL cluster to the new cluster during an upgrade operation. It coordinates the data transfer process across all databases and tablespaces based on the configured transfer mode.

## Definition
```c
void transfer_all_new_tablespaces(DbInfoArr *old_db_arr, DbInfoArr *new_db_arr,
                                 char *old_pgdata, char *new_pgdata)
```

## Detailed Description
The `transfer_all_new_tablespaces` function orchestrates the transfer of all user relation files during a PostgreSQL upgrade. It supports multiple transfer modes (clone, copy, copy_file_range, link) and can operate in both single-threaded and parallel modes. The function handles the complexity of transferring files across multiple tablespaces, ensuring that all database files are properly migrated from the old cluster to the new cluster.

In single-threaded mode, it processes all tablespaces together by passing NULL as the tablespace parameter. In parallel mode, it processes the default tablespace first, then iterates through all user-created tablespaces, allowing parallel transfer operations to improve performance.

## Parameters / Member Variables
- `old_db_arr`: Array containing information about databases in the old PostgreSQL cluster
- `new_db_arr`: Array containing information about databases in the new PostgreSQL cluster  
- `old_pgdata`: Path to the old PostgreSQL data directory
- `new_pgdata`: Path to the new PostgreSQL data directory

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status_progress](../p/prep_status_progress.md)
  - [parallel_transfer_all_new_dbs](../p/parallel_transfer_all_new_dbs.md)
  - [reap_child](../r/reap_child.md)
  - [end_progress_output](../e/end_progress_output.md)
  - [check_ok](../c/check_ok.md)
  - TRANSFER_MODE_CLONE, TRANSFER_MODE_COPY, TRANSFER_MODE_COPY_FILE_RANGE, TRANSFER_MODE_LINK
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- The function adapts its behavior based on the configured transfer mode, displaying appropriate progress messages for each mode
- In parallel mode, it processes tablespaces sequentially but allows database transfers within each tablespace to run in parallel
- The function includes proper cleanup by reaping all child processes after parallel operations complete
- Error checking is performed via check_ok() to ensure all transfer operations completed successfully

## Simplified Source

```c
void transfer_all_new_tablespaces(DbInfoArr *old_db_arr, DbInfoArr *new_db_arr,
                                 char *old_pgdata, char *new_pgdata) {
    // Display progress message based on transfer mode
    switch (user_opts.transfer_mode) {
        case TRANSFER_MODE_CLONE:
            prep_status_progress("Cloning user relation files");
            break;
        case TRANSFER_MODE_COPY:
            prep_status_progress("Copying user relation files");
            break;
        case TRANSFER_MODE_COPY_FILE_RANGE:
            prep_status_progress("Copying user relation files with copy_file_range");
            break;
        case TRANSFER_MODE_LINK:
            prep_status_progress("Linking user relation files");
            break;
    }

    // Choose transfer strategy based on job count
    if (user_opts.jobs <= 1) {
        // Single-threaded: process all tablespaces together
        parallel_transfer_all_new_dbs(old_db_arr, new_db_arr, old_pgdata, new_pgdata, NULL);
    } else {
        // Parallel mode: process default tablespace first
        parallel_transfer_all_new_dbs(old_db_arr, new_db_arr, old_pgdata, new_pgdata, old_pgdata);

        // Then process each user-created tablespace in parallel
        for (int tblnum = 0; tblnum < os_info.num_old_tablespaces; tblnum++) {
            parallel_transfer_all_new_dbs(old_db_arr, new_db_arr, old_pgdata, new_pgdata,
                                        os_info.old_tablespaces[tblnum]);
        }

        // Wait for all parallel operations to complete
        while (reap_child(true) == true)
            ;
    }

    // Finalize progress reporting and check for errors
    end_progress_output();
    check_ok();
}
```