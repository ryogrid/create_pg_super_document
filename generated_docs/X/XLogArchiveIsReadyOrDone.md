# XLogArchiveIsReadyOrDone

## Location
[src/backend/access/transam/xlogarchive.c:664-693](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogarchive.c#L664-L693)

## Overview
Checks if an XLOG segment file has either a .ready or .done status file, indicating it is either queued for archival or already archived.

## Definition
bool XLogArchiveIsReadyOrDone(const char *xlog)

## Detailed Description
XLogArchiveIsReadyOrDone determines if a WAL segment file has any archival status by checking for the presence of either .ready or .done files. The function:

- First checks for .done file indicating completed archival - returns true if found
- Then checks for .ready file indicating queued archival - returns true if found  
- Includes race condition protection by double-checking for .done files
- Returns false only if neither status file exists

This function is specifically designed for use during recovery operations where the race conditions present during normal operation are not a concern. It provides a simple way to determine if a WAL file is being managed by the archival system.

## Parameters / Member Variables
- : The name of the XLOG segment file to check for archival status

## Dependencies
- Functions called/Symbols referenced:
  - [StatusFilePath](../S/StatusFilePath.md)
- Called from (representative examples):
  - [CleanupAfterArchiveRecovery](../C/CleanupAfterArchiveRecovery.md)

## Notes and Other Information
- Designed primarily for recovery scenarios where race conditions are minimized
- Would be racy during normal operations due to concurrent archiver activity
- Similar to XLogArchiveIsBusy but with inverted logic - returns true for any archival status
- Used in recovery cleanup to identify files that are part of the archival process
- Does not create status files, only checks for existing ones

## Simplified Source

```c
// Simplified version of XLogArchiveIsReadyOrDone
bool XLogArchiveIsReadyOrDone(const char *xlog) {
    char archiveStatusPath[MAXPGPATH];

    // Check if archiver is done with this file
    StatusFilePath(archiveStatusPath, xlog, ".done");
    if (file_exists(archiveStatusPath)) {
        return true;
    }

    // Check if archiver is currently processing this file
    StatusFilePath(archiveStatusPath, xlog, ".ready");
    if (file_exists(archiveStatusPath)) {
        return true;
    }

    // Double-check for .done file to handle race conditions
    StatusFilePath(archiveStatusPath, xlog, ".done");
    if (file_exists(archiveStatusPath)) {
        return true;
    }

    // File has no archival status
    return false;
}
```

Key simplifications made:
- Abstracted `stat()` system calls into conceptual `file_exists()` checks
- Removed detailed struct stat usage for clarity
- Added descriptive comments explaining the purpose of each check
- Simplified the race condition handling explanation
- Maintained the essential three-step checking logic
- Preserved the function's core purpose of detecting archival status