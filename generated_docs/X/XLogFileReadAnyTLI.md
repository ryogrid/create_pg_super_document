# XLogFileReadAnyTLI

## Location
src/backend/access/transam/xlogrecovery.c: 4274 - 4375

## Overview
Opens a WAL segment file for reading during recovery, searching across any timeline ID listed in the expected timeline history.

## Definition
```c
static int XLogFileReadAnyTLI(XLogSegNo segno, int emode, XLogSource source)
```

## Detailed Description
This function extends the functionality of `XLogFileRead` by attempting to open a WAL segment file from any timeline that might contain the requested segment. It iterates through the list of expected timelines (stored in `expectedTLEs` or read from timeline history) in descending order, attempting to locate and open the segment from each timeline.

The function implements sophisticated logic to:
1. Prevent reading from timelines that are too old (using `curFileTLI` as a lower bound)
2. Skip timelines where the requested segment doesn't belong based on timeline boundaries
3. Try both archival storage and pg_wal directory based on the source parameter
4. Maintain timeline history information for successful reads

This is particularly useful during recovery when dealing with timeline switches, where a segment might exist in different timelines depending on the recovery scenario.

## Parameters / Member Variables
- `segno`: The WAL segment number to read
- `emode`: Error mode for reporting failures when the segment cannot be found
- `source`: Source preference (XLOG_FROM_ANY, XLOG_FROM_ARCHIVE, or XLOG_FROM_PG_WAL)

## Dependencies
- Functions called/Symbols referenced:
  - [readTimeLineHistory](../r/readTimeLineHistory.md)
  - [XLogFileRead](XLogFileRead.md)
  - XLByteToSeg
  - XLogFilePath
  - TimeLineHistoryEntry
- Called from (representative examples):
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md)

## Notes and Other Information
- This is a static function, only accessible within the xlogrecovery.c module
- Uses global variables `expectedTLEs`, `curFileTLI`, and `recoveryTargetTLI`
- Implements a backward-compatibility safety feature by preventing `curFileTLI` from going backwards
- Optimizes timeline history handling by only saving the timeline list in `expectedTLEs` when a valid segment is found
- For XLOG_FROM_ANY source, tries archival storage first, then pg_wal directory
- Provides debug logging when segments are successfully retrieved from archive
- Returns a file descriptor on success, or -1 with appropriate error reporting on failure