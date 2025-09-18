# rewind_parseTimeLineHistory

## Location
src/bin/pg_rewind/timeline.c: 28 - 129

## Overview
Parses a timeline history buffer to construct a list of timeline entries, providing a pg_rewind-specific implementation of timeline history parsing without backend dependencies.

## Definition
TimeLineHistoryEntry *rewind_parseTimeLineHistory(char *buffer, TimeLineID targetTLI, int *nentries)

## Detailed Description
This function is a copy-pasted version of the backend's readTimeLineHistory function, modified specifically for pg_rewind to work without backend functions and return a malloc'd array. It parses timeline history data from a buffer to construct a chronological list of timeline entries.

The function processes each line in the history buffer, extracting timeline IDs and their corresponding WAL switchpoints. It validates the data format and ensures timeline IDs are in increasing sequence. After parsing all existing timeline entries from the history, it creates an additional entry for the "tip" of the timeline (the current timeline that has no entry in the history file).

The parsing follows a strict format where each line contains a timeline ID followed by a WAL switchpoint location in hexadecimal format (high/low 32-bit values). Lines starting with '#' are treated as comments and skipped.

## Parameters / Member Variables
- : Input buffer containing the timeline history data to parse
- : The target timeline ID for which history is being parsed
- : Output parameter that receives the number of timeline entries found

## Dependencies
- Functions called/Symbols referenced:
  - TimeLineHistoryEntry (struct type)
  - pg_log_error_detail
  - pg_realloc
  - pg_malloc
- Called from (representative examples):
  - getTimelineHistory (in src/bin/pg_rewind/pg_rewind.c:884)

## Notes and Other Information
- This is a pg_rewind-specific adaptation of backend timeline parsing functionality
- The function exits with status 1 on any parsing errors or data validation failures
- Timeline IDs must be in strictly increasing sequence in the history file
- The target timeline ID must be greater than all timeline IDs found in the history
- Memory is allocated using pg_malloc/pg_realloc and should be freed by the caller
- The function creates an additional entry for the current timeline tip with InvalidXLogRecPtr as the end point