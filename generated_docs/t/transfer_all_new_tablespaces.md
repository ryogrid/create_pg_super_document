# transfer_all_new_tablespaces

## Location
src/bin/pg_upgrade/relfilenumber.c: 29 - 88

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
  - prep_status_progress
  - parallel_transfer_all_new_dbs
  - reap_child
  - end_progress_output
  - check_ok
  - TRANSFER_MODE_CLONE, TRANSFER_MODE_COPY, TRANSFER_MODE_COPY_FILE_RANGE, TRANSFER_MODE_LINK
- Called from (representative examples):
  - main

## Notes and Other Information
- The function adapts its behavior based on the configured transfer mode, displaying appropriate progress messages for each mode
- In parallel mode, it processes tablespaces sequentially but allows database transfers within each tablespace to run in parallel
- The function includes proper cleanup by reaping all child processes after parallel operations complete
- Error checking is performed via check_ok() to ensure all transfer operations completed successfully