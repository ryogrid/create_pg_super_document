# existsTimeLineHistory

## Location
[src/backend/access/transam/timeline.c:222-263](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/timeline.c#L222-L263)

## Overview
Probes whether a timeline history file exists for a given timeline ID without actually reading its contents.

## Definition
```c
bool existsTimeLineHistory(TimeLineID probeTLI)
```

## Detailed Description
This function checks for the existence of a timeline history file for the specified timeline ID. It provides a lightweight way to determine if a timeline has a history file without the overhead of parsing the file contents.

The function handles the special case of timeline 1 (master timeline) by immediately returning false, since the master timeline never has a history file. For other timelines, it constructs the appropriate file path (either from archive during recovery or from the local pg_wal directory) and attempts to open the file for reading. If the file can be opened successfully, it immediately closes it and returns true. If the file cannot be found, it returns false. Any other file access errors result in a FATAL error.

## Parameters / Member Variables
- `probeTLI`: The timeline ID to check for history file existence

## Dependencies
- Functions called/Symbols referenced:
  - [TLHistoryFileName](../T/TLHistoryFileName.md) - constructs timeline history filename
  - [RestoreArchivedFile](../R/RestoreArchivedFile.md) - attempts to restore archived file during recovery
  - [TLHistoryFilePath](../T/TLHistoryFilePath.md) - constructs local path to history file
  - [AllocateFile](../A/AllocateFile.md) - attempts to open the history file
  - [FreeFile](../F/FreeFile.md) - closes the file if successfully opened
  - MAXFNAMELEN - constant for maximum filename length
- Called from (representative examples):
  - [findNewestTimeLine](../f/findNewestTimeLine.md) - [when](../w/when.md) searching for the newest available timeline
  - [validateRecoveryParameters](../v/validateRecoveryParameters.md) - during recovery parameter validation
  - [WalRcvFetchTimeLineHistoryFiles](../W/WalRcvFetchTimeLineHistoryFiles.md) - during WAL receiver operations

## Notes and Other Information
- Timeline 1 (master timeline) always returns false as it has no history file
- The function only checks for file existence, not file validity or contents
- During archive recovery, it may trigger restoration of the history file from archive
- File access errors other than ENOENT (file not found) result in FATAL errors
- Located in src/backend/access/transam/timeline.c:222-263

## Simplified Source

```c
// Simplified version of existsTimeLineHistory
bool existsTimeLineHistory(TimeLineID probeTLI) {
    char path[MAXPGPATH];
    char histfname[MAXFNAMELEN];
    FILE *fd;

    // Timeline 1 has no history file by design
    if (probeTLI == 1)
        return false;

    // Get the timeline history file path
    if (ArchiveRecoveryRequested) {
        // During recovery: try to restore from archive
        TLHistoryFileName(histfname, probeTLI);
        RestoreArchivedFile(path, histfname, "RECOVERYHISTORY", 0, false);
    } else {
        // Normal operation: use local path
        TLHistoryFilePath(path, probeTLI);
    }

    // Check if file exists by trying to open it
    fd = AllocateFile(path, "r");
    if (fd != NULL) {
        FreeFile(fd);
        return true;
    }

    // File doesn't exist or access error
    if (errno != ENOENT) {
        // Fatal error for unexpected file access problems
        ereport(FATAL, (errcode_for_file_access(),
                       errmsg("could not open file \"%s\": %m", path)));
    }

    return false;
}
```

Key simplifications made:
- Consolidated variable declarations for clarity
- Added descriptive comments for each major logic step
- Clarified the two different path resolution strategies (archive vs local)
- Simplified the file existence check logic flow
- Maintained all essential error handling while making it more readable