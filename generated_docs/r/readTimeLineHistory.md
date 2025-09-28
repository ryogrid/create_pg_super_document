# readTimeLineHistory

## Location
[src/backend/access/transam/timeline.c:76-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/timeline.c#L76-L221)

## Overview
Reads and parses a timeline history file to construct a list of timeline entries representing the branching history of a specific timeline.

## Definition
```c
List *readTimeLineHistory(TimeLineID targetTLI)
```

## Detailed Description
This function reads a timeline history file for a given timeline ID and constructs a list of TimeLineHistoryEntry structures representing the complete timeline history. Timeline history files contain information about when timelines were created and their switchpoint locations in the WAL.

The function handles several scenarios:
1. For timeline 1 (master timeline), returns a single entry since it has no history file
2. During archive recovery, attempts to restore the history file from archive first
3. Parses the history file line by line, extracting timeline IDs and switchpoints
4. Validates the timeline sequence to ensure increasing order
5. Creates an additional "tip" entry for the current timeline
6. Returns a list ordered with newest timeline first

The history file format consists of lines containing: timeline_id, switchpoint_location, and optional comments.

## Parameters / Member Variables
- `targetTLI`: The timeline ID for which to read the history file

## Dependencies
- Functions called/Symbols referenced:
  - [TLHistoryFileName](../T/TLHistoryFileName.md) - constructs timeline history filename
  - [RestoreArchivedFile](../R/RestoreArchivedFile.md) - restores archived file during recovery
  - [TLHistoryFilePath](../T/TLHistoryFilePath.md) - constructs local path to history file
  - [AllocateFile](../A/AllocateFile.md) - opens the history file for reading
  - [FreeFile](../F/FreeFile.md) - closes the file handle
  - [palloc](../p/palloc.md) - allocates memory for timeline entries
  - [lcons](../l/lcons.md) - prepends entries to the result list
  - [KeepFileRestoredFromArchive](../K/KeepFileRestoredFromArchive.md) - preserves restored archive file
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/end - reports wait events for monitoring
- Called from (representative examples):
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md) - during WAL recovery
  - [rescanLatestTimeLine](rescanLatestTimeLine.md) - [when](../w/when.md) rescanning timeline information
  - [XLogFileReadAnyTLI](../X/XLogFileReadAnyTLI.md) - [when](../w/when.md) reading WAL files across timelines
  - [StartReplication](../S/StartReplication.md) - during replication setup
  - various backup and WAL summarizer functions

## Notes and Other Information
- Timeline 1 (master timeline) has no history file and is handled specially
- The function validates timeline ID ordering to detect corrupted history files
- Switchpoints are stored as 64-bit LSN values constructed from high/low 32-bit parts
- The result list is built with newest timeline entries first
- Comments and empty lines in history files are ignored during parsing
- Located in src/backend/access/transam/timeline.c:76-221

## Simplified Source

```c
// Simplified version of readTimeLineHistory
List *readTimeLineHistory(TimeLineID targetTLI) {
    List *result;
    char path[MAXPGPATH];
    char histfname[MAXFNAMELEN];
    FILE *fd;
    TimeLineHistoryEntry *entry;
    XLogRecPtr prevend = InvalidXLogRecPtr;
    bool fromArchive = false;

    // Timeline 1 has no history file - return single entry
    if (targetTLI == 1) {
        entry = (TimeLineHistoryEntry *) palloc(sizeof(TimeLineHistoryEntry));
        entry->tli = targetTLI;
        entry->begin = entry->end = InvalidXLogRecPtr;
        return list_make1(entry);
    }

    // Try to get history file from archive or local filesystem
    if (ArchiveRecoveryRequested) {
        TLHistoryFileName(histfname, targetTLI);
        fromArchive = RestoreArchivedFile(path, histfname, "RECOVERYHISTORY", 0, false);
    } else {
        TLHistoryFilePath(path, targetTLI);
    }

    // Open the history file
    fd = AllocateFile(path, "r");
    if (fd == NULL) {
        if (errno != ENOENT) {
            ereport(FATAL, (errcode_for_file_access(),
                          errmsg("could not open file \"%s\": %m", path)));
        }
        // No history file - return single entry
        entry = (TimeLineHistoryEntry *) palloc(sizeof(TimeLineHistoryEntry));
        entry->tli = targetTLI;
        entry->begin = entry->end = InvalidXLogRecPtr;
        return list_make1(entry);
    }

    result = NIL;

    // Parse the history file line by line
    for (;;) {
        char fline[MAXPGPATH];
        char *res;
        TimeLineID tli;
        uint32 switchpoint_hi, switchpoint_lo;
        int nfields;

        // Read next line
        pgstat_report_wait_start(WAIT_EVENT_TIMELINE_HISTORY_READ);
        res = fgets(fline, sizeof(fline), fd);
        pgstat_report_wait_end();

        if (res == NULL) {
            if (ferror(fd)) {
                ereport(ERROR, (errcode_for_file_access(),
                              errmsg("could not read file \"%s\": %m", path)));
            }
            break; // End of file
        }

        // Skip whitespace and comments
        char *ptr;
        for (ptr = fline; *ptr && isspace((unsigned char) *ptr); ptr++);
        if (*ptr == '\0' || *ptr == '#') {
            continue;
        }

        // Parse timeline ID and switchpoint
        nfields = sscanf(fline, "%u\t%X/%X", &tli, &switchpoint_hi, &switchpoint_lo);
        if (nfields != 3) {
            ereport(FATAL, (errmsg("syntax error in history file: %s", fline)));
        }

        // Create and add timeline entry
        entry = (TimeLineHistoryEntry *) palloc(sizeof(TimeLineHistoryEntry));
        entry->tli = tli;
        entry->begin = prevend;
        entry->end = ((uint64) switchpoint_hi) << 32 | (uint64) switchpoint_lo;
        prevend = entry->end;

        result = lcons(entry, result); // Add to front of list
    }

    FreeFile(fd);

    // Add entry for the target timeline itself
    entry = (TimeLineHistoryEntry *) palloc(sizeof(TimeLineHistoryEntry));
    entry->tli = targetTLI;
    entry->begin = prevend;
    entry->end = InvalidXLogRecPtr;
    result = lcons(entry, result);

    // Keep archived file if needed
    if (fromArchive) {
        KeepFileRestoredFromArchive(path, histfname);
    }

    return result;
}
```

Key simplifications made:
- Removed detailed error validation for timeline ordering
- Simplified comment parsing logic
- Consolidated variable declarations
- Added clear step-by-step comments
- Removed complex validation checks (preserved essential error handling)
- Focused on the main algorithm flow