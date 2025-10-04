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

## Simplified Source

```c
static XLogRecPtr SummarizeWAL(TimeLineID tli, XLogRecPtr start_lsn, bool exact,
                              XLogRecPtr switch_lsn, XLogRecPtr maximum_lsn)
{
    XLogReaderState *xlogreader;
    XLogRecPtr summary_start_lsn, summary_end_lsn = switch_lsn;
    char temp_path[MAXPGPATH], final_path[MAXPGPATH];
    WalSummaryIO io;
    BlockRefTable *brtab = CreateEmptyBlockRefTable();
    bool fast_forward = true;

    // Initialize WAL reader for the specified timeline
    SummarizerReadLocalXLogPrivate *private_data =
        palloc0(sizeof(SummarizerReadLocalXLogPrivate));
    private_data->tli = tli;
    private_data->historic = !XLogRecPtrIsInvalid(switch_lsn);
    private_data->read_upto = maximum_lsn;

    xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
                                   XL_ROUTINE(.page_read = &summarizer_read_local_xlog_page,
                                             .segment_open = &wal_segment_open,
                                             .segment_close = &wal_segment_close),
                                   private_data);

    // Find starting position for summarization
    if (exact) {
        XLogBeginRead(xlogreader, start_lsn);
        summary_start_lsn = start_lsn;
    } else {
        summary_start_lsn = XLogFindNextRecord(xlogreader, start_lsn);
        if (XLogRecPtrIsInvalid(summary_start_lsn)) {
            // Handle end of WAL or invalid position
            if (private_data->end_of_wal) {
                summary_start_lsn = start_lsn;
                summary_end_lsn = private_data->read_upto;
                switch_lsn = xlogreader->EndRecPtr;
            } else {
                ereport(ERROR, (errmsg("could not find a valid record after %X/%X",
                                      LSN_FORMAT_ARGS(start_lsn))));
            }
        }
    }

    // Main loop: read WAL records and build block reference table
    while (1) {
        char *errormsg;
        XLogRecord *record;

        HandleWalSummarizerInterrupts();

        // Read next WAL record
        record = XLogReadRecord(xlogreader, &errormsg);
        if (record == NULL) {
            if (private_data->end_of_wal) {
                summary_end_lsn = private_data->read_upto;
                break;
            }
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not read WAL from timeline %u at %X/%X",
                                 tli, LSN_FORMAT_ARGS(xlogreader->EndRecPtr))));
        }

        // Check for timeline switch boundary
        if (!XLogRecPtrIsInvalid(switch_lsn) && xlogreader->ReadRecPtr >= switch_lsn) {
            summary_end_lsn = switch_lsn;
            break;
        }

        // Handle special record types (checkpoints, etc.)
        uint8 rmid = XLogRecGetRmid(xlogreader);
        if (rmid == RM_XLOG_ID) {
            bool new_fast_forward;
            if (SummarizeXlogRecord(xlogreader, &new_fast_forward)) {
                if (xlogreader->ReadRecPtr > summary_start_lsn) {
                    summary_end_lsn = xlogreader->ReadRecPtr;
                    break;
                } else {
                    fast_forward = new_fast_forward;
                }
            }
        } else if (!fast_forward) {
            // Handle database/storage manager/transaction records
            switch (rmid) {
                case RM_DBASE_ID:
                    SummarizeDbaseRecord(xlogreader, brtab);
                    break;
                case RM_SMGR_ID:
                    SummarizeSmgrRecord(xlogreader, brtab);
                    break;
                case RM_XACT_ID:
                    SummarizeXactRecord(xlogreader, brtab);
                    break;
            }
        }

        // Extract block references from record if not in fast-forward mode
        if (!fast_forward) {
            for (int block_id = 0; block_id <= XLogRecMaxBlockId(xlogreader); block_id++) {
                RelFileLocator rlocator;
                ForkNumber forknum;
                BlockNumber blocknum;

                if (!XLogRecGetBlockTagExtended(xlogreader, block_id, &rlocator,
                                               &forknum, &blocknum, NULL))
                    continue;

                // Skip FSM fork (not fully WAL-logged)
                if (forknum != FSM_FORKNUM)
                    BlockRefTableMarkBlockModified(brtab, &rlocator, forknum, blocknum);
            }
        }

        summary_end_lsn = xlogreader->EndRecPtr;

        // Update shared memory with progress
        LWLockAcquire(WALSummarizerLock, LW_EXCLUSIVE);
        WalSummarizerCtl->pending_lsn = summary_end_lsn;
        LWLockRelease(WALSummarizerLock);

        // Check if we've reached the switch point
        if (!XLogRecPtrIsInvalid(switch_lsn) && xlogreader->EndRecPtr >= switch_lsn)
            break;
    }

    // Cleanup WAL reader
    pfree(xlogreader->private_data);
    XLogReaderFree(xlogreader);

    // Write summary file if we made progress and not in fast-forward mode
    if (summary_end_lsn > summary_start_lsn && !fast_forward) {
        // Generate file paths
        snprintf(temp_path, MAXPGPATH, XLOGDIR "/summaries/temp.summary");
        snprintf(final_path, MAXPGPATH,
                XLOGDIR "/summaries/%08X%08X%08X%08X%08X.summary",
                tli, LSN_FORMAT_ARGS(summary_start_lsn), LSN_FORMAT_ARGS(summary_end_lsn));

        // Write summary file
        io.filepos = 0;
        io.file = PathNameOpenFile(temp_path, O_WRONLY | O_CREAT | O_TRUNC);
        WriteBlockRefTable(brtab, WriteWalSummary, &io);
        FileClose(io.file);

        // Atomically rename to final location
        durable_rename(temp_path, final_path, ERROR);

        ereport(DEBUG1, (errmsg_internal("summarized WAL on TLI %u from %X/%X to %X/%X",
                                        tli, LSN_FORMAT_ARGS(summary_start_lsn),
                                        LSN_FORMAT_ARGS(summary_end_lsn))));
    }

    return summary_end_lsn;
}
```