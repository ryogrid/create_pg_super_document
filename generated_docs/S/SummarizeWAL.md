# SummarizeWAL

## Location
[src/backend/postmaster/walsummarizer.c:906-1245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/walsummarizer.c#L906-L1245)

## Overview
Summarizes a range of WAL records on a single timeline, creating a compact representation of modified blocks and storing it as a WAL summary file.

## Definition
```c
static XLogRecPtr SummarizeWAL(TimeLineID tli, XLogRecPtr start_lsn, bool exact,
                              XLogRecPtr switch_lsn, XLogRecPtr maximum_lsn)
```

## Detailed Description
The SummarizeWAL function is the core component of PostgreSQL's WAL summarization process, responsible for reading WAL records from a specific timeline and generating summary files that track modified blocks. This function operates by reading WAL records sequentially, extracting block references, and maintaining a block reference table that records which blocks have been modified. The summarization process is crucial for incremental backup functionality, allowing efficient identification of changed blocks without scanning the entire WAL stream.

The function handles various edge cases including timeline switches, checkpoint records, and different WAL levels. It operates in two modes: normal mode where it tracks all block modifications, and "fast forward" mode where it skips summarization (typically when wal_level=minimal is encountered). The function creates temporary summary files during processing and atomically renames them to their final location upon successful completion.

## Parameters / Member Variables
- `tli`: Timeline ID of the timeline to be summarized
- `start_lsn`: Starting LSN for summarization; behavior depends on the `exact` parameter
- `exact`: If true, `start_lsn` is treated as exact record boundary; if false, function searches forward for next valid record
- `switch_lsn`: LSN at which to switch to a later timeline (for historic timelines); InvalidXLogRecPtr for current timeline
- `maximum_lsn`: Maximum LSN that can be safely read; represents switch point for historic timelines or current end of WAL

## Dependencies
- Functions called/Symbols referenced:
  - [CreateEmptyBlockRefTable](../C/CreateEmptyBlockRefTable.md): Creates empty block reference table for tracking modifications
  - [XLogReaderAllocate](../X/XLogReaderAllocate.md): Allocates WAL reader state structure
  - [XLogBeginRead](../X/XLogBeginRead.md)/XLogFindNextRecord: Initialize WAL reading from specified position
  - [XLogReadRecord](../X/XLogReadRecord.md): Read individual WAL records
  - [SummarizeXlogRecord](SummarizeXlogRecord.md): Handle special XLOG record types (checkpoints, etc.)
  - [SummarizeDbaseRecord](SummarizeDbaseRecord.md)/SummarizeSmgrRecord/SummarizeXactRecord: Handle specific record types
  - [BlockRefTableMarkBlockModified](../B/BlockRefTableMarkBlockModified.md): Mark blocks as modified in the reference table
  - [WriteBlockRefTable](../W/WriteBlockRefTable.md): Write the block reference table to summary file
- Called from (representative examples):
  - [WalSummarizerMain](../W/WalSummarizerMain.md): Main entry point for WAL summarizer process

## Notes and Other Information
- The function implements sophisticated timeline handling, supporting both current and historic timelines
- Creates summary files with names containing timeline ID, start LSN, and end LSN for easy identification
- Handles timeline switches gracefully by stopping summarization at the appropriate boundary
- Uses temporary files during writing to ensure atomicity of summary file creation
- Skips FSM (Free Space Map) fork modifications as they are not fully WAL-logged
- Implements "fast forward" mode to skip summarization when WAL level is minimal
- Updates shared memory structures to track summarization progress
- Returns the actual ending LSN of the summary, which may differ from the requested range due to various stopping conditions