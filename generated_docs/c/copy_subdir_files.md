# copy_subdir_files

## Location
src/bin/pg_upgrade/pg_upgrade.c: 677 - 701

## Overview
Copies the contents of a subdirectory from the old PostgreSQL cluster to the corresponding subdirectory in the new cluster during pg_upgrade.

## Definition


## Detailed Description
This function performs a complete directory copy operation from the old cluster to the new cluster during pg_upgrade. It first removes the existing subdirectory in the new cluster (including the directory itself), then copies the entire contents of the corresponding subdirectory from the old cluster.

The function uses platform-specific copy commands:
- On Unix/Linux systems: Uses  command for recursive copy with force overwrite
- On Windows systems: Uses  command with equivalent functionality (everything, no confirm, quiet, overwrite read-only)

The copy operation preserves the directory structure and file permissions from the old cluster. The function provides user feedback and ensures proper error handling throughout the process.

## Parameters / Member Variables
- : The name of the subdirectory in the old cluster's data directory to copy from
- : The name of the subdirectory in the new cluster's data directory to copy to (can be the same as old_subdir or different)

## Dependencies
- Functions called/Symbols referenced:
  - remove_new_subdir: Removes the target subdirectory from the new cluster before copying
  - prep_status: Displays status message to user about the copy operation  
  - exec_prog: Executes the platform-specific copy command
  - check_ok: Verifies the copy operation completed successfully
- Global variables used:
  - old_cluster.pgdata: Path to the old cluster's data directory
  - new_cluster.pgdata: Path to the new cluster's data directory
  - UTILITY_LOG_FILE: Log file for utility operations
- Called from:
  - copy_xact_xlog_xid: Multiple times to copy transaction-related directories

## Notes and Other Information
- The function completely replaces the target directory - it doesn't merge contents
- Platform-specific implementation ensures compatibility across different operating systems
- The remove operation (remove_new_subdir) with rmtopdir=true ensures a clean slate before copying
- Error handling ensures that copy failures will abort the pg_upgrade process
- Commonly used to copy critical directories like pg_xact, pg_multixact, and other transaction state directories
- The function assumes both source and destination parent directories already exist
- File permissions and timestamps are typically preserved by the underlying copy commands