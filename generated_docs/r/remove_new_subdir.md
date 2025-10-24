# remove_new_subdir

## Location
[src/bin/pg_upgrade/pg_upgrade.c:660-676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.c#L660-L676)

## Overview
Deletes the contents of a specified subdirectory from the new PostgreSQL cluster's data directory during the pg_upgrade process.

## Definition

```c
static void
remove_new_subdir(const char *subdir, bool rmtopdir)
```
## Detailed Description
This function removes files and directories from a specified subdirectory within the new cluster's data directory. It is used during pg_upgrade to clean up directories that need to be replaced with data from the old cluster. The function constructs the full path by combining the new cluster's pgdata directory with the provided subdirectory name, then uses the rmtree utility function to perform the actual deletion.

The function provides user feedback through prep_status() and includes error handling to report deletion failures. It ensures the operation completed successfully using check_ok().

## Parameters / Member Variables
- `*subdir`: The name of the subdirectory to delete (relative to the new cluster's pgdata directory)
- `rmtopdir`: Boolean flag indicating whether to remove the top-level directory itself (true) or just its contents (false)
## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md): Displays status message to user about the deletion operation
  - [rmtree](rmtree.md): Utility function that recursively removes directories and their contents
  - [check_ok](../c/check_ok.md): Verifies the operation completed successfully
- Global variables used:
  - new_cluster.pgdata: Path to the new cluster's data directory
- Called from:
  - [copy_subdir_files](../c/copy_subdir_files.md): Before copying files from old to new cluster
  - [copy_xact_xlog_xid](../c/copy_xact_xlog_xid.md): Before copying transaction log data

## Notes and Other Information
- The function is primarily used to clean up directories in the new cluster before copying corresponding data from the old cluster
- The rmtopdir parameter allows flexible control over whether the directory structure is preserved or completely removed
- Error handling ensures that pg_upgrade fails cleanly if directory deletion is unsuccessful
- The function is typically called as a preparation step before copying replacement data from the old cluster
- Common subdirectories that might be removed include pg_xact, pg_multixact, and other transaction-related directories

## Simplified Source

```c
static void remove_new_subdir(const char *subdir, bool rmtopdir) {
    char new_path[MAXPGPATH];

    prep_status("Deleting files from new %s", subdir);

    // Build full path to target directory
    snprintf(new_path, sizeof(new_path), "%s/%s", new_cluster.pgdata, subdir);

    // Remove directory and its contents
    if (!rmtree(new_path, rmtopdir))
        pg_fatal("could not delete directory \"%s\"", new_path);

    check_ok();
}
```