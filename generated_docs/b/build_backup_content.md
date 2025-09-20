# build_backup_content

## Location
[src/backend/access/transam/xlogbackup.c:29-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogbackup.c#L29-L94)

## Overview
Builds the contents for backup_label or backup history files, formatting backup metadata into a string representation that can be written to these special files during PostgreSQL backup operations.

## Definition

```c
char *
build_backup_content(BackupState *state, bool ishistoryfile)
```
## Detailed Description
The  function creates formatted string content for two types of backup-related files in PostgreSQL:

1. **backup_label file**: Created during active backups to record backup state information
2. **backup history file**: Created after backup completion to maintain a historical record

The function takes a  structure containing all necessary backup metadata and formats it into a standardized text format. The output includes critical information such as WAL locations, timelines, checkpoint locations, and backup method details.

When  is false, it generates content for an active backup_label file. When true, it includes additional information specific to backup history files, such as stop time and stop timeline information.

The function uses the log timezone (not session timezone) for timestamp formatting to ensure consistency across different client sessions.

## Parameters / Member Variables
- : Pointer to BackupState structure containing backup metadata including start/stop points, timelines, checkpoint location, backup name, and timing information
- : Boolean flag determining output format - true for backup history file content, false for backup_label file content

## Dependencies
- Functions called/Symbols referenced:
  - : Creates dynamic string buffer for building output
  - : Formats timestamps using PostgreSQL's timezone-aware formatting
  - : Converts time_t to local time structure using specified timezone
  - : Converts WAL byte position to segment number
  - : Generates WAL file name from timeline and segment number
  - : Checks if WAL record pointer is valid
  - : Structure containing backup state information
  - : Type for WAL segment numbers
  - : Maximum filename length constant

- Called from (representative examples):
  - : Generates backup_label content when stopping backup
  - : Creates backup files during base backup operations
  - : Used in backup stop function implementation

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- Uses log_timezone for timestamp formatting to maintain consistency regardless of client session timezone
- Handles both incremental and full backup scenarios through optional incremental start point fields
- The generated content follows a specific format that is expected by PostgreSQL recovery processes
- Contains assertions to ensure data consistency, particularly for incremental backup parameters
- The function is located in src/backend/access/transam/xlogbackup.c:29-94
- Output format includes standardized fields like START WAL LOCATION, CHECKPOINT LOCATION, BACKUP METHOD (always "streamed"), START TIME, LABEL, and timeline information