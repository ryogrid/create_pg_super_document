# tar_sync

## Location
src/bin/pg_basebackup/walmethods.c: 1017 - 1041

## Overview
Synchronizes a TAR-based WAL file to persistent storage by flushing the entire TAR file to disk.

## Definition
```c
static int tar_sync(Walfile *f)
```

## Detailed Description
This function implements the sync operation for TAR-based WAL writing method in pg_basebackup. It ensures that WAL data written to a TAR file is synchronized to persistent storage. The function performs several important checks:

1. Verifies that sync is enabled for the method
2. Skips sync for compressed files (as it makes no sense to sync partial compressed data)
3. Syncs the entire TAR file using fsync() when applicable

The function takes a conservative approach by always syncing the entire TAR file rather than individual entries, which is the only meaningful operation possible with TAR files.

## Parameters / Member Variables
- `f`: Pointer to the Walfile structure representing the open WAL file within the TAR method

## Dependencies
- Functions called/Symbols referenced:
  - clear_error
  - fsync
  - TarMethodData (structure type)
  - Walfile (structure type)
  - PG_COMPRESSION_NONE (constant)
- Called from (representative examples):
  - CreateWalDirectoryMethod (function pointer assignment)
  - tar_close

## Notes and Other Information
- This function is marked as static, meaning it's only accessible within the walmethods.c file
- Returns 0 on success or if sync is disabled/not applicable
- Returns the result of fsync() (typically -1 on error) and sets lasterrno appropriately
- Compressed files are explicitly skipped because partial sync of compressed data is meaningless
- Uses Assert() to ensure the Walfile pointer is not NULL
- The function syncs the entire TAR file rather than individual WAL file entries