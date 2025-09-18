# getTimelineHistory

## Location
src/bin/pg_rewind/pg_rewind.c: 856 - 918

## Overview
getTimelineHistory retrieves timeline history information for either the source or target PostgreSQL system during pg_rewind operations.

## Definition


## Detailed Description
This function fetches timeline history information, which is essential for understanding the branching points in PostgreSQL's WAL timeline structure. The function handles two scenarios:

1. **Timeline 1 (Primary timeline)**: Since timeline 1 has no history file, it creates a fake entry with infinite start and end positions using InvalidXLogRecPtr values.

2. **Other timelines**: For timelines > 1, it reads the actual timeline history file from either the source system (via the replication connection) or the target data directory, then parses the content into TimeLineHistoryEntry structures.

The function also provides debug output when debug mode is enabled, displaying the timeline entries with their begin and end LSN positions.

## Parameters / Member Variables
- : TimeLineID specifying which timeline's history to retrieve
- : Boolean flag indicating whether to fetch from source system (true) or target directory (false)
- : Output parameter returning the number of history entries found

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc (memory allocation)
  - TLHistoryFilePath (constructs timeline history file path)
  - source->fetch_file (fetches file from source system via replication)
  - slurpFile (reads file from local target directory)
  - rewind_parseTimeLineHistory (parses timeline history file content)
  - pg_free (memory deallocation)
  - pg_log_debug (debug logging)
- Called from (representative examples):
  - main (called twice to get both source and target timeline histories)

## Notes and Other Information
- This is a static function local to pg_rewind.c
- Timeline 1 is special-cased because it represents the original timeline and has no history file
- The function abstracts the difference between fetching from a remote source vs local target
- Debug output shows timeline ID and begin/end LSN ranges in hexadecimal format
- Memory allocated for history entries must be freed by the caller
- Part of pg_rewind's timeline analysis phase to determine divergence points