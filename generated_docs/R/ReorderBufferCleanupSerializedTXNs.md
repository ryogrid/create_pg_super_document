# ReorderBufferCleanupSerializedTXNs

## Location
src/backend/replication/logical/reorderbuffer.c: 4728 - 4766

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
  - AllocateDir (directory handle allocation)
  - ReadDirExtended (directory reading with error handling)
  - unlink (file deletion)
  - FreeDir (directory handle cleanup)
  - ereport/ERROR (error reporting)
  - errcode_for_file_access (error code generation)
- Called from (representative examples):
  - ReorderBufferAllocate
  - ReorderBufferFree
  - StartupReorderBuffer

## Notes and Other Information
- This is a static function used internally within the reorderbuffer.c module
- The function is typically called during replication slot initialization or cleanup
- File names must start with "xid" to be considered for deletion (transaction spill files)
- The function safely handles the case where the slot directory doesnt exist or isnt a directory
- Critical for preventing accumulation of orphaned spill files after crashes or abnormal termination
- Uses ReadDirExtended with INFO level logging for better error handling during directory scanning
- Path construction uses pg_replslot/[slotname]/xid* pattern for file identification