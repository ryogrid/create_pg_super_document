# check_file_clone

## Location
[src/bin/pg_upgrade/file.c:360-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/file.c#L360-L399)

## Overview
Tests whether file cloning/reflinking is supported between the old and new PostgreSQL data directories, ensuring compatibility before attempting to use cloning during the upgrade process.

## Definition

```c
void
check_file_clone(void)
```
## Detailed Description
The check_file_clone function performs a compatibility test to determine if file cloning operations can be successfully performed between the old and new PostgreSQL data directories. This test is crucial during pg_upgrade's pre-flight checks to ensure that the cloning method can be used for efficient file transfers.

The function creates a test scenario by attempting to clone the  file from the old cluster's data directory to a temporary test file () in the new cluster's data directory. This test validates several important conditions:

1. **Cross-directory cloning**: Ensures cloning works between the old and new data directories
2. **Filesystem compatibility**: Verifies that both directories are on filesystems that support cloning/reflinking
3. **Permission compatibility**: Confirms that the pg_upgrade process has appropriate permissions

The function uses the same platform-specific implementations as  (macOS copyfile with COPYFILE_CLONE_FORCE, Linux FICLONE ioctl) and fails gracefully if cloning is not supported on the platform.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - unlink
  - copyfile (macOS implementation)
  - open
  - ioctl (Linux implementation)
  - close
  - [pg_fatal](../p/pg_fatal.md)
  - old_cluster.pgdata
  - new_cluster.pgdata
  - MAXPGPATH
  - PG_BINARY
  - pg_file_create_mode
  - COPYFILE_CLONE_FORCE
  - FICLONE
- Called from (representative examples):
  - [check_new_cluster](check_new_cluster.md)

## Notes and Other Information
- Called during pg_upgrade's pre-flight checks to validate cloning capability
- Uses the same conditional compilation flags as  to ensure consistency
- Creates a temporary test file that is cleaned up regardless of success or failure
- Terminates pg_upgrade with a fatal error if cloning is not supported but was expected to be available
- The test file  is automatically removed after the test completes
- Essential for determining the optimal file transfer method during upgrade (clone vs. copy vs. link)
- Helps pg_upgrade make informed decisions about transfer strategies based on actual filesystem capabilities
- Platform-specific implementations match those used in the actual cloning operations
- The function assumes that if cloning works for the PG_VERSION file, it will work for all relation files in the same directories