# writeTimeLineHistory

## Location
[src/backend/access/transam/timeline.c:304-462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/timeline.c#L304-L462)

## Overview
Creates a new timeline history file by copying the parent timeline history and appending information about the new timeline split.

## Definition
```c
void writeTimeLineHistory(TimeLineID newTLI, TimeLineID parentTLI, XLogRecPtr switchpoint, char *reason)
```

## Detailed Description
This function creates a timeline history file for a new timeline during operations like standby promotion or timeline switching. The process involves several critical steps:

1. **Temporary file creation**: Creates a temporary file with a unique name to avoid corruption during write
2. **Parent history copying**: If the parent timeline has a history file, copies its entire contents verbatim to preserve the complete timeline ancestry
3. **New entry addition**: Appends a new line containing the parent timeline ID, switchpoint LSN, and human-readable reason for the timeline split
4. **Atomic completion**: Uses durable_rename to atomically move the completed file to its final location
5. **Archive notification**: Notifies the archiver that the new history file is ready for archival

The function includes robust error handling throughout, cleaning up temporary files on failure and providing detailed error messages. It also uses proper wait event reporting for performance monitoring.

## Parameters / Member Variables
- `newTLI`: The timeline ID of the new timeline being created
- `parentTLI`: The timeline ID of the immediate parent timeline
- `switchpoint`: The WAL location (LSN) where the system switched to the new timeline
- `reason`: A human-readable explanation of why the timeline was switched

## Dependencies
- Functions called/Symbols referenced:
  - [OpenTransientFile](../O/OpenTransientFile.md) - opens temporary and source files
  - [CloseTransientFile](../C/CloseTransientFile.md) - closes file handles
  - [TLHistoryFileName](../T/TLHistoryFileName.md) - constructs timeline history filename
  - [TLHistoryFilePath](../T/TLHistoryFilePath.md) - constructs local path to history file
  - [RestoreArchivedFile](../R/RestoreArchivedFile.md) - restores parent history from archive if needed
  - [durable_rename](../d/durable_rename.md) - atomically renames temporary file to final location
  - XLogArchivingActive - checks if archiving is enabled
  - [XLogArchiveNotify](../X/XLogArchiveNotify.md) - notifies archiver of new file
  - [pg_fsync](../p/pg_fsync.md) - ensures data is written to disk
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/end - reports wait events for monitoring
  - Various system calls: read, write, unlink, access
- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md) - during database startup when creating new timelines

## Notes and Other Information
- Used primarily at the end of recovery operations when promoting a standby or switching timelines
- The function ensures atomic file creation using temporary files and durable_rename
- Parent timeline history is preserved by copying the entire parent history file
- Includes comprehensive error handling with cleanup of temporary files on failure
- Timeline history files are immediately eligible for archival upon creation
- The new timeline ID must be greater than the parent timeline ID (enforced by assertion)
- Located in src/backend/access/transam/timeline.c:304-462

## Simplified Source

```c
// Simplified version of writeTimeLineHistory
void writeTimeLineHistory(TimeLineID newTLI, TimeLineID parentTLI,
                         XLogRecPtr switchpoint, char *reason)
{
    char temp_path[MAXPGPATH];
    char final_path[MAXPGPATH];
    char history_filename[MAXFNAMELEN];
    char buffer[BLCKSZ];
    int temp_fd, source_fd, nbytes;

    Assert(newTLI > parentTLI);

    // Create temporary file for safe writing
    snprintf(temp_path, MAXPGPATH, XLOGDIR "/xlogtemp.%d", (int) getpid());
    unlink(temp_path);
    temp_fd = OpenTransientFile(temp_path, O_RDWR | O_CREAT | O_EXCL);
    if (temp_fd < 0)
        ereport(ERROR, "could not create temp file");

    // Copy parent timeline history if it exists
    if (ArchiveRecoveryRequested) {
        TLHistoryFileName(history_filename, parentTLI);
        RestoreArchivedFile(final_path, history_filename, "RECOVERYHISTORY", 0, false);
    } else {
        TLHistoryFilePath(final_path, parentTLI);
    }

    source_fd = OpenTransientFile(final_path, O_RDONLY);
    if (source_fd >= 0) {
        // Copy parent history content in chunks
        for (;;) {
            nbytes = read(source_fd, buffer, sizeof(buffer));
            if (nbytes <= 0) break;

            if (write(temp_fd, buffer, nbytes) != nbytes) {
                unlink(temp_path);
                ereport(ERROR, "could not write to temp file");
            }
        }
        CloseTransientFile(source_fd);
    }

    // Append new timeline entry
    snprintf(buffer, sizeof(buffer), "%s%u\t%X/%X\t%s\n",
             (source_fd < 0) ? "" : "\n",
             parentTLI, LSN_FORMAT_ARGS(switchpoint), reason);

    nbytes = strlen(buffer);
    if (write(temp_fd, buffer, nbytes) != nbytes) {
        unlink(temp_path);
        ereport(ERROR, "could not write timeline entry");
    }

    // Ensure data is on disk
    pg_fsync(temp_fd);
    CloseTransientFile(temp_fd);

    // Atomically move to final location
    TLHistoryFilePath(final_path, newTLI);
    durable_rename(temp_path, final_path, ERROR);

    // Notify archiver if archiving is active
    if (XLogArchivingActive()) {
        TLHistoryFileName(history_filename, newTLI);
        XLogArchiveNotify(history_filename);
    }
}
```

Key simplifications made:
- Removed detailed error handling patterns for clarity (kept essential error checks)
- Consolidated repetitive write error handling into single pattern
- Simplified variable declarations and removed some intermediate variables
- Abstracted wait event reporting calls (pgstat_report_wait_start/end)
- Streamlined the file copy loop by removing errno manipulation
- Maintained core algorithm: temp file → copy parent → append new entry → atomic rename
- Preserved essential functionality: file creation, copying, appending, syncing, and archiving