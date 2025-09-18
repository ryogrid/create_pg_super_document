# remove_new_subdir

## Location
src/bin/pg_upgrade/pg_upgrade.c: 660 - 676

## Overview
Deletes the contents of a specified subdirectory from the new PostgreSQL cluster's data directory during the pg_upgrade process.

## Definition


## Detailed Description
This function removes files and directories from a specified subdirectory within the new cluster's data directory. It is used during pg_upgrade to clean up directories that need to be replaced with data from the old cluster. The function constructs the full path by combining the new cluster's pgdata directory with the provided subdirectory name, then uses the rmtree utility function to perform the actual deletion.

The function provides user feedback through prep_status() and includes error handling to report deletion failures. It ensures the operation completed successfully using check_ok().

## Parameters / Member Variables
- : The name of the subdirectory to delete (relative to the new cluster's pgdata directory)
- : Boolean flag indicating whether to remove the top-level directory itself (true) or just its contents (false)

## Dependencies
- Functions called/Symbols referenced:
  - prep_status: Displays status message to user about the deletion operation
  - rmtree: Utility function that recursively removes directories and their contents
  - check_ok: Verifies the operation completed successfully
- Global variables used:
  - new_cluster.pgdata: Path to the new cluster's data directory
- Called from:
  - copy_subdir_files: Before copying files from old to new cluster
  - copy_xact_xlog_xid: Before copying transaction log data

## Notes and Other Information
- The function is primarily used to clean up directories in the new cluster before copying corresponding data from the old cluster
- The rmtopdir parameter allows flexible control over whether the directory structure is preserved or completely removed
- Error handling ensures that pg_upgrade fails cleanly if directory deletion is unsuccessful
- The function is typically called as a preparation step before copying replacement data from the old cluster
- Common subdirectories that might be removed include pg_xact, pg_multixact, and other transaction-related directories