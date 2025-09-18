# xl_heap_insert

## Location
src/include/access/heapam_xlog.h: 159 - 165

## Overview
The xl_heap_insert struct represents the WAL record data for heap tuple insertion operations in PostgreSQL's recovery and replication system.

## Definition


## Detailed Description
This structure contains the metadata needed to record and replay heap tuple insertion operations in PostgreSQL's WAL system. When a new tuple is inserted into a heap table, this record is written to the WAL along with the actual tuple header (xl_heap_header) and tuple data as a backup block. The structure itself is compact, containing only the essential metadata, while the bulk of the insertion data (tuple header and content) is stored separately in the WAL record's backup block.

## Parameters / Member Variables
- : The offset number (position) where the new tuple was inserted within its page
- : Control flags that provide additional context about the insertion operation (such as whether it's a speculative insertion)

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a data structure)
- Called from (representative examples):
  - heap_insert (creates WAL records for tuple insertions)
  - heap_xlog_insert (replays insertion from WAL records during recovery)
  - heap_desc (describes insertion WAL records for debugging purposes)
  - DecodeInsert (logical replication decoding of insert operations)

## Notes and Other Information
- The actual tuple header (xl_heap_header) and tuple data are stored in backup block 0, not in this structure
- The SizeOfHeapInsert macro provides the size of this structure
- Used for both regular insertions and speculative insertions (controlled by flags)
- Essential for crash recovery, point-in-time recovery, and streaming replication
- The compact design minimizes WAL overhead while providing all necessary replay information
- Works in conjunction with backup blocks to completely capture insertion operations