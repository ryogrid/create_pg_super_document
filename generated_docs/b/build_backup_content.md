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
- `*state`: Pointer to BackupState structure containing backup metadata including start/stop points, timelines, checkpoint location, backup name, and timing information
- `ishistoryfile`: Boolean flag determining output format - true for backup history file content, false for backup_label file content
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

## Simplified Source

```c
// Simplified version of build_backup_content
char *build_backup_content(BackupState *state, bool ishistoryfile) {
    char startstrbuf[128];
    char startxlogfile[MAXFNAMELEN];
    XLogSegNo startsegno;
    StringInfo result = makeStringInfo();
    char *data;

    Assert(state != NULL);

    // Format start time using log timezone
    pg_strftime(startstrbuf, sizeof(startstrbuf), "%Y-%m-%d %H:%M:%S %Z",
                pg_localtime(&state->starttime, log_timezone));

    // Generate WAL filename for start position
    XLByteToSeg(state->startpoint, startsegno, wal_segment_size);
    XLogFileName(startxlogfile, state->starttli, startsegno, wal_segment_size);
    appendStringInfo(result, "START WAL LOCATION: %X/%X (file %s)\n",
                     LSN_FORMAT_ARGS(state->startpoint), startxlogfile);

    // Add stop information for history files
    if (ishistoryfile) {
        char stopxlogfile[MAXFNAMELEN];
        XLogSegNo stopsegno;

        XLByteToSeg(state->stoppoint, stopsegno, wal_segment_size);
        XLogFileName(stopxlogfile, state->stoptli, stopsegno, wal_segment_size);
        appendStringInfo(result, "STOP WAL LOCATION: %X/%X (file %s)\n",
                         LSN_FORMAT_ARGS(state->stoppoint), stopxlogfile);
    }

    // Add standard backup metadata
    appendStringInfo(result, "CHECKPOINT LOCATION: %X/%X\n",
                     LSN_FORMAT_ARGS(state->checkpointloc));
    appendStringInfoString(result, "BACKUP METHOD: streamed\n");
    appendStringInfo(result, "BACKUP FROM: %s\n",
                     state->started_in_recovery ? "standby" : "primary");
    appendStringInfo(result, "START TIME: %s\n", startstrbuf);
    appendStringInfo(result, "LABEL: %s\n", state->name);
    appendStringInfo(result, "START TIMELINE: %u\n", state->starttli);

    // Add stop time and timeline for history files
    if (ishistoryfile) {
        char stopstrfbuf[128];

        pg_strftime(stopstrfbuf, sizeof(stopstrfbuf), "%Y-%m-%d %H:%M:%S %Z",
                    pg_localtime(&state->stoptime, log_timezone));
        appendStringInfo(result, "STOP TIME: %s\n", stopstrfbuf);
        appendStringInfo(result, "STOP TIMELINE: %u\n", state->stoptli);
    }

    // Add incremental backup information if present
    Assert(XLogRecPtrIsInvalid(state->istartpoint) == (state->istarttli == 0));
    if (!XLogRecPtrIsInvalid(state->istartpoint)) {
        appendStringInfo(result, "INCREMENTAL FROM LSN: %X/%X\n",
                         LSN_FORMAT_ARGS(state->istartpoint));
        appendStringInfo(result, "INCREMENTAL FROM TLI: %u\n",
                         state->istarttli);
    }

    // Return the formatted content
    data = result->data;
    pfree(result);
    return data;
}
```

Key simplifications made:
- Removed detailed comments while preserving formatting logic
- Streamlined the conditional sections for history vs label files
- Maintained timezone handling for consistent timestamps
- Preserved WAL filename generation and LSN formatting
- Kept incremental backup support and validation assertions