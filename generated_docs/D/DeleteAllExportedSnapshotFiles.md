# DeleteAllExportedSnapshotFiles

## Location
[src/backend/utils/time/snapmgr.c:1567-1605](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1567-L1605)

## Overview
DeleteAllExportedSnapshotFiles cleans up snapshot export files left behind by crashed backend processes during database startup or recovery.

## Definition
```c
void DeleteAllExportedSnapshotFiles(void)
```

## Detailed Description
This function provides cleanup functionality for exported snapshot files that may have been left behind by backend processes that crashed before properly cleaning up their exported snapshots. It operates during database startup or crash recovery phases.

The function performs the following operations:
1. **Directory Scanning**: Opens and scans the SNAPSHOT_EXPORT_DIR directory
2. **File Enumeration**: Iterates through all files in the export directory, skipping "." and ".." entries
3. **File Removal**: Attempts to unlink (delete) each snapshot file found
4. **Error Handling**: Reports any file operation errors at LOG level rather than ERROR level to avoid preventing database startup

The function uses conservative error handling - problems with reading the directory or removing files are logged but don't prevent database startup, as cleanup failures are not critical enough to block the system.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - SNAPSHOT_EXPORT_DIR (directory constant)
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDirExtended](../R/ReadDirExtended.md)
  - unlink
  - [FreeDir](../F/FreeDir.md)
  - [DIR](DIR.md) (directory structure)
  - [dirent](../d/dirent.md) (directory entry structure)
- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md) (during database startup and crash recovery)

## Notes and Other Information
- Called during database startup or crash recovery to clean up orphaned snapshot files
- Uses LOG level error reporting to avoid preventing database startup
- Removes all files from SNAPSHOT_EXPORT_DIR indiscriminately (assumes all files are orphaned)
- Conservative approach: cleanup failures don't block system startup
- Part of PostgreSQL's crash recovery and cleanup mechanisms

## Simplified Source

```c
// Simplified version of DeleteAllExportedSnapshotFiles
void DeleteAllExportedSnapshotFiles(void) {
    char filepath_buffer[MAXPGPATH + sizeof(SNAPSHOT_EXPORT_DIR)];
    DIR *snapshot_dir;
    struct dirent *dir_entry;

    // Open the snapshot export directory
    snapshot_dir = AllocateDir(SNAPSHOT_EXPORT_DIR);

    // Iterate through all files in the directory
    while ((dir_entry = ReadDirExtended(snapshot_dir, SNAPSHOT_EXPORT_DIR, LOG)) != NULL) {
        // Skip current and parent directory entries
        if (strcmp(dir_entry->d_name, ".") == 0 ||
            strcmp(dir_entry->d_name, "..") == 0) {
            continue;
        }

        // Build full file path
        snprintf(filepath_buffer, sizeof(filepath_buffer),
                 SNAPSHOT_EXPORT_DIR "/%s", dir_entry->d_name);

        // Attempt to delete the file, log errors but don't fail
        if (unlink(filepath_buffer) != 0) {
            ereport(LOG, (errcode_for_file_access(),
                         errmsg("could not remove file \"%s\": %m", filepath_buffer)));
        }
    }

    // Clean up directory handle
    FreeDir(snapshot_dir);
}
```

Key simplifications made:
- Used more descriptive variable names (filepath_buffer, snapshot_dir, dir_entry)
- Added inline comments explaining each major step
- Simplified the conditional logic formatting for better readability
- Preserved all essential functionality including error handling
- Maintained the LOG-level error reporting approach
- Kept the conservative error handling that doesn't block startup