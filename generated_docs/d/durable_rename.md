# durable_rename

## Location
[src/common/file_utils.c:461-524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/file_utils.c#L461-L524)

## Overview
A crash-safe wrapper around the rename(2) system call that ensures the rename operation persists across system crashes by performing necessary fsync operations.

## Definition

```c
int
durable_rename(const char *oldfile, const char *newfile)
```
## Detailed Description
 provides ACID-compliant file renaming by ensuring that both the source and destination files, as well as the parent directory metadata, are synchronized to persistent storage before and after the rename operation. The function follows a careful sequence: it first fsyncs the old file and any existing target file, performs the actual rename, then fsyncs the renamed file and its parent directory. This guarantees that in case of a crash, either the old file exists in its original location or the new file exists in the target location, with no possibility of data loss or corruption. The function cannot rename across different filesystems since rename(2) doesn't support cross-filesystem operations.

## Parameters / Member Variables
- : Path to the source file to be renamed
- : Path to the destination file name
- : Error reporting level for logging errors (e.g., ERROR, WARNING, LOG)

## Dependencies
- Functions called/Symbols referenced:
  - [fsync_fname_ext](../f/fsync_fname_ext.md)
  - [OpenTransientFile](../O/OpenTransientFile.md)
  - PG_BINARY
  - [pg_fsync](../p/pg_fsync.md)
  - [CloseTransientFile](../C/CloseTransientFile.md)
  - rename
  - [fsync_parent_path](../f/fsync_parent_path.md)
- Called from (representative examples):
  - [writeTimeLineHistory](../w/writeTimeLineHistory.md)
  - [writeTimeLineHistoryFile](../w/writeTimeLineHistoryFile.md)
  - [InstallXLogFileSegment](../I/InstallXLogFileSegment.md)
  - [CleanupAfterArchiveRecovery](../C/CleanupAfterArchiveRecovery.md)
  - [StartupXLOG](../S/StartupXLOG.md)
  - [KeepFileRestoredFromArchive](../K/KeepFileRestoredFromArchive.md)
  - [write_relmap_file](../w/write_relmap_file.md)
  - [AlterSystemSetConfigFile](../A/AlterSystemSetConfigFile.md)

## Notes and Other Information
This function is critical for PostgreSQL's crash recovery and data durability guarantees. It's extensively used in WAL (Write-Ahead Logging) operations, configuration file updates, and other scenarios where file operations must survive system crashes. The function returns 0 on success and -1 on failure, with errno not guaranteed to be valid upon return. There's also a simpler version in src/common/file_utils.c used by client utilities. The careful fsync sequence ensures that the rename operation is atomic from a durability perspective, even though the underlying rename(2) system call itself is not crash-safe without explicit synchronization.

## Simplified Source

```c
// Simplified version of durable_rename
int durable_rename(const char *oldfile, const char *newfile, int elevel) {
    // Step 1: Sync the old file to ensure it's persistent
    if (fsync_fname_ext(oldfile, false, false, elevel) != 0)
        return -1;

    // Step 2: Sync the target file if it exists (makes crash behavior predictable)
    int fd = OpenTransientFile(newfile, PG_BINARY | O_RDWR);
    if (fd >= 0) {
        // Target file exists, sync it before rename
        if (pg_fsync(fd) != 0 || CloseTransientFile(fd) != 0) {
            // Handle sync/close errors
            return -1;
        }
    }
    // If target doesn't exist (ENOENT), that's fine - continue

    // Step 3: Perform the actual rename operation
    if (rename(oldfile, newfile) < 0) {
        // Report rename failure
        return -1;
    }

    // Step 4: Ensure the renamed file is persistent
    if (fsync_fname_ext(newfile, false, false, elevel) != 0)
        return -1;

    // Step 5: Sync the parent directory to persist the directory entry
    if (fsync_parent_path(newfile, elevel) != 0)
        return -1;

    return 0;
}
```

Key simplifications made:
- Removed detailed error handling and reporting for clarity
- Consolidated file open/sync/close logic for target file
- Abstracted the specific error codes and errno handling
- Focused on the five main steps of the durability protocol
- Simplified the conditional logic while preserving the core algorithm