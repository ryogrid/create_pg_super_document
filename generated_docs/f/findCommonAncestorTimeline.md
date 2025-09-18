# findCommonAncestorTimeline

## Location
[src/bin/pg_rewind/pg_rewind.c:919-960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/pg_rewind.c#L919-L960)

## Overview
findCommonAncestorTimeline determines the timeline ID of the last common timeline shared between two PostgreSQL clusters and identifies the divergence point in their WAL histories.

## Definition


## Detailed Description
This function performs a critical analysis for pg_rewind by comparing the timeline histories of two PostgreSQL clusters to find their common ancestor timeline. The function traces through both timeline histories entry by entry, looking for the point where they diverge.

The algorithm works by:
1. Comparing corresponding timeline entries from both histories
2. Checking both timeline ID and begin position for exact matches
3. Stopping at the first mismatch found
4. Setting the divergence point to the end of the last common timeline using MinXLogRecPtr
5. Fatally erroring if no common ancestor is found

The function handles edge cases where clusters might have used the same timeline number but with different start positions, which can occur due to different recovery processes and history file fetching patterns.

## Parameters / Member Variables
- : Array of TimeLineHistoryEntry structures for the first cluster (source)
- : Number of entries in the first cluster's timeline history
- : Array of TimeLineHistoryEntry structures for the second cluster (target)  
- : Number of entries in the second cluster's timeline history
- : Output parameter set to the WAL record position where timelines diverged
- : Output parameter set to the index of the last common timeline in the history arrays

## Dependencies
- Functions called/Symbols referenced:
  - Min (standard minimum macro)
  - [MinXLogRecPtr](../M/MinXLogRecPtr.md) (specialized minimum function for XLogRecPtr values)
  - [pg_fatal](../p/pg_fatal.md) (fatal error reporting)
- Called from (representative examples):
  - [main](../m/main.md) (part of the main pg_rewind timeline analysis process)

## Notes and Other Information
- This is a static function local to pg_rewind.c
- The function will terminate the program with a fatal error if no common ancestor is found
- Critical for determining the rewind target point during PostgreSQL data directory synchronization
- Handles complex scenarios where timeline numbers might be reused with different begin positions
- The divergence point calculation uses MinXLogRecPtr to handle potential invalid LSN values correctly
- Part of pg_rewind's core algorithm for identifying the point from which to start copying changes