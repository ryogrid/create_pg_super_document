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

## Simplified Source

```c
void check_file_clone(void) {
    char existing_file[MAXPGPATH];
    char new_link_file[MAXPGPATH];

    // Set up test files: PG_VERSION from old cluster, test clone in new cluster
    snprintf(existing_file, sizeof(existing_file), "%s/PG_VERSION", old_cluster.pgdata);
    snprintf(new_link_file, sizeof(new_link_file), "%s/PG_VERSION.clonetest", new_cluster.pgdata);
    unlink(new_link_file);  // Clean up any previous test file

#if defined(HAVE_COPYFILE) && defined(COPYFILE_CLONE_FORCE)
    // macOS: Use copyfile with clone flag
    if (copyfile(existing_file, new_link_file, NULL, COPYFILE_CLONE_FORCE) < 0)
        pg_fatal("could not clone file between old and new data directories: %m");

#elif defined(__linux__) && defined(FICLONE)
    // Linux: Use ioctl with FICLONE
    int src_fd = open(existing_file, O_RDONLY | PG_BINARY, 0);
    if (src_fd < 0)
        pg_fatal("could not open file \"%s\": %m", existing_file);

    int dest_fd = open(new_link_file, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, pg_file_create_mode);
    if (dest_fd < 0)
        pg_fatal("could not create file \"%s\": %m", new_link_file);

    // Attempt the actual clone operation
    if (ioctl(dest_fd, FICLONE, src_fd) < 0)
        pg_fatal("could not clone file between old and new data directories: %m");

    close(src_fd);
    close(dest_fd);

#else
    // Platform doesn't support cloning
    pg_fatal("file cloning not supported on this platform");
#endif

    // Clean up test file
    unlink(new_link_file);
}
```