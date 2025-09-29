# ReorderBufferCleanupSerializedTXNs

## Location
[src/backend/replication/logical/reorderbuffer.c:4728-4766](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L4728-L4766)

## Overview
ReorderBufferCleanupSerializedTXNs removes leftover serialized reorder buffer files from a replication slot directory, typically called after crashes or when cleaning up after decoding sessions.

## Definition
```c
static void ReorderBufferCleanupSerializedTXNs(const char *slotname)
```

## Detailed Description
This function performs cleanup operations on a replication slot directory by removing any leftover serialized transaction files that may exist from previous logical decoding sessions or after system crashes. It scans the slot directory for files with names starting with "xid" (transaction ID files) and removes them systematically. The function is designed to handle recovery scenarios where spill files might be left behind due to unexpected termination.

The function first checks if the slot directory exists and is indeed a directory, then iterates through all directory entries looking for transaction spill files. It constructs full file paths and attempts to delete each matching file, reporting errors if deletion fails.

## Parameters / Member Variables
- `slotname`: Name of the replication slot whose serialized transaction files should be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - lstat (file system status check)
  - S_ISDIR (directory check macro)
  - [AllocateDir](../A/AllocateDir.md) (directory handle allocation)
  - [ReadDirExtended](ReadDirExtended.md) (directory reading with error handling)
  - unlink (file deletion)
  - [FreeDir](../F/FreeDir.md) (directory handle cleanup)
  - ereport/ERROR (error reporting)
  - [errcode_for_file_access](../e/errcode_for_file_access.md) (error code generation)
- Called from (representative examples):
  - [ReorderBufferAllocate](ReorderBufferAllocate.md)
  - [ReorderBufferFree](ReorderBufferFree.md)
  - [StartupReorderBuffer](../S/StartupReorderBuffer.md)

## Notes and Other Information
- This is a static function used internally within the reorderbuffer.c module
- The function is typically called during replication slot initialization or cleanup
- File names must start with "xid" to be considered for deletion (transaction spill files)
- The function safely handles the case where the slot directory doesnt exist or isnt a directory
- Critical for preventing accumulation of orphaned spill files after crashes or abnormal termination
- Uses ReadDirExtended with INFO level logging for better error handling during directory scanning
- [Path](../P/Path.md) construction uses pg_replslot/[slotname]/xid* pattern for file identification

## Simplified Source

```c
// Simplified version of ReorderBufferCleanupSerializedTXNs
static void ReorderBufferCleanupSerializedTXNs(const char *slotname)
{
    DIR *spill_dir;
    struct dirent *spill_de;
    char path[MAXPGPATH * 2 + 12];

    // Build path to replication slot directory
    sprintf(path, "pg_replslot/%s", slotname);

    // Skip if path is not a directory
    if (!directory_exists(path))
        return;

    // Open slot directory for reading
    spill_dir = AllocateDir(path);

    // Scan directory for transaction spill files
    while ((spill_de = ReadDirExtended(spill_dir, path, INFO)) != NULL)
    {
        // Look for files starting with "xid" (transaction files)
        if (strncmp(spill_de->d_name, "xid", 3) == 0)
        {
            // Build full path to spill file
            snprintf(path, sizeof(path), "pg_replslot/%s/%s",
                    slotname, spill_de->d_name);

            // Remove the spill file
            if (unlink(path) != 0)
                ereport(ERROR,
                       (errcode_for_file_access(),
                        errmsg("could not remove file \"%s\": %m", path)));
        }
    }

    // Clean up directory handle
    FreeDir(spill_dir);
}
```

Key simplifications made:
- Abstracted `lstat()` and `S_ISDIR()` check into conceptual `directory_exists()` function
- Simplified error message to focus on core issue
- Added descriptive comments for each major step
- Maintained all essential logic including error handling
- Preserved the file naming pattern check ("xid" prefix)
- Kept the critical unlink error reporting for robustness