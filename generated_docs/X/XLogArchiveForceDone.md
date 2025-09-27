# XLogArchiveForceDone

## Location
[src/backend/access/transam/xlogarchive.c:510-564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogarchive.c#L510-L564)

## Overview
Forces the creation of an archive completion notification file (.done) to indicate that a WAL segment has been successfully archived, bypassing the normal archival process.

## Definition
void XLogArchiveForceDone(const char *xlog)

## Detailed Description
XLogArchiveForceDone forcibly creates a .done status file for a given WAL segment file, indicating that the segment has been successfully archived. This function bypasses the normal archive process and creates the completion notification regardless of whether a .ready file exists. 

The function first checks if a .done file already exists and exits early if found. If a .ready file exists, it renames it to .done using durable_rename for atomic operation. If no .ready file exists, it creates an empty .done file directly. This forced completion is typically used in recovery scenarios or when manually managing archive states.

## Parameters / Member Variables
- : The name of the XLOG segment file for which to create the archive done notification

## Dependencies
- Functions called/Symbols referenced:
  - [StatusFilePath](../S/StatusFilePath.md)
  - [durable_rename](../d/durable_rename.md)  
  - [AllocateFile](../A/AllocateFile.md)
  - [FreeFile](../F/FreeFile.md)
- Called from (representative examples):
  - [KeepFileRestoredFromArchive](../K/KeepFileRestoredFromArchive.md)
  - [WalReceiverMain](../W/WalReceiverMain.md)
  - [WalRcvFetchTimeLineHistoryFiles](../W/WalRcvFetchTimeLineHistoryFiles.md)
  - [XLogWalRcvClose](XLogWalRcvClose.md)

## Notes and Other Information
- Creates .done files in the archive_status directory under pg_wal
- Uses durable_rename for atomic file operations when a .ready file exists
- Logs warnings but continues execution if file operations fail
- Primarily used during WAL recovery and replication processes
- The function ensures idempotent behavior by checking for existing .done files

## Simplified Source

```c
// Simplified version of XLogArchiveForceDone
void XLogArchiveForceDone(const char *xlog) {
    char archiveReady[MAXPGPATH];
    char archiveDone[MAXPGPATH];
    struct stat stat_buf;
    FILE *fd;

    // Step 1: Check if .done file already exists
    StatusFilePath(archiveDone, xlog, ".done");
    if (stat(archiveDone, &stat_buf) == 0) {
        return; // Already marked as done
    }

    // Step 2: Try to rename existing .ready file to .done
    StatusFilePath(archiveReady, xlog, ".ready");
    if (stat(archiveReady, &stat_buf) == 0) {
        durable_rename(archiveReady, archiveDone, WARNING);
        return;
    }

    // Step 3: Create empty .done file if no .ready exists
    fd = AllocateFile(archiveDone, "w");
    if (fd == NULL) {
        // Log error and continue
        ereport(LOG, (errcode_for_file_access(),
                     errmsg("could not create archive status file \"%s\": %m",
                            archiveDone)));
        return;
    }

    if (FreeFile(fd)) {
        // Log error and continue
        ereport(LOG, (errcode_for_file_access(),
                     errmsg("could not write archive status file \"%s\": %m",
                            archiveDone)));
        return;
    }
}
```

Key simplifications made:
- Added step-by-step comments to clarify the three main phases
- Kept essential error handling but simplified error reporting comments
- Maintained the core logic flow: check existing → rename ready → create new
- Preserved all critical functionality while improving readability
- Consolidated variable declarations for clarity