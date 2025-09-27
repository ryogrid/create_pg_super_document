# WalRcvFetchTimeLineHistoryFiles

## Location
[src/backend/replication/walreceiver.c:745-800](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiver.c#L745-L800)

## Overview
WalRcvFetchTimeLineHistoryFiles fetches missing timeline history files from the primary server for all timelines in a specified range.

## Definition
static void WalRcvFetchTimeLineHistoryFiles(TimeLineID first, TimeLineID last)

## Detailed Description
This function ensures that the standby server has all necessary timeline history files for timelines between the specified first and last timeline IDs (inclusive). Timeline history files are crucial for PostgreSQL's point-in-time recovery and timeline management, as they contain the branching history of database timelines.

The function operates by:
1. **Range iteration**: Loops through all timeline IDs from first to last
2. **Existence check**: Uses existsTimeLineHistory() to determine if each timeline history file is already present locally
3. **Selective fetching**: Only fetches files that are missing locally (timeline 1 never has a history file)
4. **Remote retrieval**: Uses walrcv_readtimelinehistoryfile() to fetch content from the primary server
5. **Validation**: Compares the filename provided by the primary with the expected local filename for consistency
6. **Local storage**: Writes the fetched content to the local pg_wal directory using writeTimeLineHistoryFile()
7. **Archive handling**: Appropriately marks files for archiving based on the archive_mode setting

The function includes safety checks to ensure protocol compliance - if the primary server reports an unexpected filename for a timeline history file, it raises a protocol violation error. This helps detect potential issues with timeline consistency between primary and standby servers.

## Parameters / Member Variables
- : The starting timeline ID in the range to check and fetch
wtmp begins Sun Aug 20 19:22:10 2023: The ending timeline ID in the range to check and fetch (inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - [existsTimeLineHistory](../e/existsTimeLineHistory.md)
  - walrcv_readtimelinehistoryfile
  - [TLHistoryFileName](../T/TLHistoryFileName.md)
  - [writeTimeLineHistoryFile](../w/writeTimeLineHistoryFile.md)
  - [XLogArchiveForceDone](../X/XLogArchiveForceDone.md), XLogArchiveNotify
  - ereport, errmsg, errmsg_internal
  - strcmp, pfree
  - MAXFNAMELEN, ARCHIVE_MODE_ALWAYS

- Called from (representative examples):
  - [WalReceiverMain](WalReceiverMain.md) (during streaming setup and timeline changes)
  - WalRcvWakeupReason

## Notes and Other Information
- Critical for maintaining timeline consistency in streaming replication
- Prevents timeline ID collisions when the standby is later promoted to primary
- Timeline 1 never has a history file as it represents the initial timeline
- Handles both archive_mode settings appropriately for fetched files
- Part of PostgreSQL's timeline management and point-in-time recovery infrastructure
- Fetched files are immediately available for use by the recovery process

## Simplified Source

```c
// Simplified version of WalRcvFetchTimeLineHistoryFiles
static void WalRcvFetchTimeLineHistoryFiles(TimeLineID first, TimeLineID last) {
    TimeLineID tli;

    // Iterate through timeline range
    for (tli = first; tli <= last; tli++) {
        // Skip timeline 1 (no history file) and existing files
        if (tli != 1 && !existsTimeLineHistory(tli)) {
            char *filename;
            char *content;
            int length;
            char expected_filename[MAXFNAMELEN];

            // Log the fetch operation
            ereport(LOG, (errmsg("fetching timeline history file for timeline %u from primary server", tli)));

            // Fetch file from primary server
            walrcv_readtimelinehistoryfile(wrconn, tli, &filename, &content, &length);

            // Validate filename matches expectation
            TLHistoryFileName(expected_filename, tli);
            if (strcmp(filename, expected_filename) != 0) {
                ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                    errmsg_internal("primary reported unexpected file name for timeline history file of timeline %u", tli)));
            }

            // Write file to local pg_wal directory
            writeTimeLineHistoryFile(tli, content, length);

            // Handle archiving based on archive mode
            if (XLogArchiveMode != ARCHIVE_MODE_ALWAYS)
                XLogArchiveForceDone(filename);
            else
                XLogArchiveNotify(filename);

            // Clean up allocated memory
            pfree(filename);
            pfree(content);
        }
    }
}
```

Key simplifications made:
- Renamed variables for clarity (fname→filename, len→length)
- Added descriptive comments for each major step
- Preserved all essential logic including error handling and archiving
- Maintained the core algorithm structure
- Kept critical safety checks for protocol compliance