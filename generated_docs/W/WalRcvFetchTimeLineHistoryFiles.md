# WalRcvFetchTimeLineHistoryFiles

## Location
src/backend/replication/walreceiver.c: 745 - 800

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
- 
wtmp begins Sun Aug 20 19:22:10 2023: The ending timeline ID in the range to check and fetch (inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - existsTimeLineHistory
  - walrcv_readtimelinehistoryfile
  - TLHistoryFileName
  - writeTimeLineHistoryFile
  - XLogArchiveForceDone, XLogArchiveNotify
  - ereport, errmsg, errmsg_internal
  - strcmp, pfree
  - MAXFNAMELEN, ARCHIVE_MODE_ALWAYS

- Called from (representative examples):
  - WalReceiverMain (during streaming setup and timeline changes)
  - WalRcvWakeupReason

## Notes and Other Information
- Critical for maintaining timeline consistency in streaming replication
- Prevents timeline ID collisions when the standby is later promoted to primary
- Timeline 1 never has a history file as it represents the initial timeline
- Handles both archive_mode settings appropriately for fetched files
- Part of PostgreSQL's timeline management and point-in-time recovery infrastructure
- Fetched files are immediately available for use by the recovery process