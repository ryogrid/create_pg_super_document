# PrepareForIncrementalBackup

## Location
[src/backend/backup/basebackup_incremental.c:265-626](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_incremental.c#L265-L626)

## Overview
Validates manifest data and prepares the WAL summary infrastructure required to perform an incremental backup by analyzing timeline history, verifying WAL ranges, and building an in-memory block reference table.

## Definition

```c
struct IncrementalBackupInfo for some thoughts on
	 * memory usage.
	 */
	ib->brtab = CreateEmptyBlockRefTable();
```
## Detailed Description
This function is the core preparation step for incremental backups, called after the backup manifest has been parsed via AppendIncrementalManifestData and FinalizeIncrementalManifest. It performs comprehensive validation and preparation:

1. **Timeline Validation**: Matches WAL ranges from the backup manifest against the server's timeline history to ensure the incremental backup is based on a valid previous state of this server.

2. **WAL Range Analysis**: Identifies the earliest and latest WAL ranges from the manifest, determining the complete LSN range that needs to be covered by WAL summaries.

3. **Sanity Checking**: Validates that WAL ranges align properly with timeline boundaries and that no ranges extend beyond the current backup's start point.

4. **WAL Summary Collection**: Waits for WAL summarization to catch up, then retrieves and validates that all required WAL summaries are available for the needed LSN ranges.

5. **Block Reference Table Construction**: Reads all required WAL summary files and merges their block reference information into a single in-memory table that will be used to determine which blocks need to be included in the incremental backup.

The function ensures that the incremental backup will be consistent and complete by validating the entire chain of dependencies from the prior backup to the current backup start point.

## Parameters / Member Variables
- : IncrementalBackupInfo structure containing parsed manifest data and where the final block reference table will be stored
- : BackupState structure that will be updated with timeline and LSN information from the prior backup

## Dependencies
- Functions called/Symbols referenced:
  - [readTimeLineHistory](../r/readTimeLineHistory.md): Reads timeline history for validation
  - [WaitForWalSummarization](../W/WaitForWalSummarization.md): Ensures WAL summarization is caught up
  - [GetWalSummaries](../G/GetWalSummaries.md), FilterWalSummaries: Retrieves and filters WAL summary files
  - [WalSummariesAreComplete](../W/WalSummariesAreComplete.md): Validates completeness of WAL summaries
  - [CreateEmptyBlockRefTable](../C/CreateEmptyBlockRefTable.md): Initializes block reference table
  - [OpenWalSummaryFile](../O/OpenWalSummaryFile.md), CreateBlockRefTableReader: Reads WAL summary files
  - [BlockRefTableReaderNextRelation](../B/BlockRefTableReaderNextRelation.md), BlockRefTableReaderGetBlocks: Parses summary data
  - [BlockRefTableSetLimitBlock](../B/BlockRefTableSetLimitBlock.md), BlockRefTableMarkBlockModified: Builds block reference table
- Types referenced:
  - [IncrementalBackupInfo](../I/IncrementalBackupInfo.md), BackupState, TimeLineHistoryEntry
  - [backup_wal_range](../b/backup_wal_range.md), WalSummaryFile, WalSummaryIO, BlockRefTableReader
- Called from:
  - [perform_base_backup](../p/perform_base_backup.md) (src/backend/backup/basebackup.c:287)

## Notes and Other Information
- This function performs extensive error checking and will throw detailed error messages if the manifest is inconsistent with the server's timeline history
- The function handles complex timeline scenarios including timeline switches that may have occurred during the prior backup
- Memory management is handled through the IncrementalBackupInfo's memory context
- The block reference table built by this function can be quite large, as noted in the IncrementalBackupInfo structure comments
- WAL summarization must be enabled and functional for this function to succeed
- The function provides helpful hints for common issues like rate-limiting on standby servers

## Simplified Source

```c
// Simplified version of PrepareForIncrementalBackup
void PrepareForIncrementalBackup(IncrementalBackupInfo *ib, BackupState *backup_state) {
    MemoryContext oldcontext;
    List *expectedTLEs, *all_wslist, *required_wslist = NIL;
    int num_wal_ranges, i;
    bool found_backup_start_tli = false;
    TimeLineID earliest_wal_range_tli = 0, latest_wal_range_tli = 0;
    XLogRecPtr earliest_wal_range_start_lsn = InvalidXLogRecPtr;
    TimeLineHistoryEntry **tlep;

    // Switch to incremental backup memory context
    oldcontext = MemoryContextSwitchTo(ib->mcxt);

    // Validate manifest contains WAL ranges
    num_wal_ranges = list_length(ib->manifest_wal_ranges);
    if (num_wal_ranges == 0)
        ereport(ERROR, "manifest contains no required WAL ranges");

    // Read server's timeline history for validation
    expectedTLEs = readTimeLineHistory(backup_state->starttli);
    tlep = palloc0(num_wal_ranges * sizeof(TimeLineHistoryEntry *));

    // Match manifest WAL ranges with server timeline history
    for (i = 0; i < num_wal_ranges; ++i) {
        backup_wal_range *range = list_nth(ib->manifest_wal_ranges, i);
        bool saw_earliest = false, saw_latest = false;

        // Find this range's timeline in server history
        foreach(lc, expectedTLEs) {
            TimeLineHistoryEntry *tle = lfirst(lc);
            if (tle->tli == range->tli) {
                tlep[i] = tle;
                break;
            }
            // Track timeline ordering for earliest/latest determination
            if (tle->tli == earliest_wal_range_tli) saw_earliest = true;
            if (tle->tli == latest_wal_range_tli) saw_latest = true;
        }

        // Error if timeline not found in server history
        if (tlep[i] == NULL)
            ereport(ERROR, "timeline %u found in manifest, but not in server history", range->tli);

        // Update earliest and latest timeline tracking
        if (!saw_latest) latest_wal_range_tli = range->tli;
        if (earliest_wal_range_tli == 0 || saw_earliest) {
            earliest_wal_range_tli = range->tli;
            earliest_wal_range_start_lsn = range->start_lsn;
        }
    }

    // Set backup state with prior backup information
    backup_state->istartpoint = earliest_wal_range_start_lsn;
    backup_state->istarttli = earliest_wal_range_tli;

    // Validate WAL range LSN boundaries against timeline history
    for (i = 0; i < num_wal_ranges; ++i) {
        backup_wal_range *range = list_nth(ib->manifest_wal_ranges, i);

        // Check start LSN alignment with timeline boundaries
        if (range->tli == earliest_wal_range_tli) {
            if (range->start_lsn < tlep[i]->begin)
                ereport(ERROR, "manifest requires WAL before timeline begins");
        } else {
            if (range->start_lsn != tlep[i]->begin)
                ereport(ERROR, "manifest WAL start doesn't match timeline begin");
        }

        // Check end LSN alignment
        if (range->tli == latest_wal_range_tli) {
            if (range->end_lsn > backup_state->startpoint)
                ereport(ERROR, "manifest WAL extends beyond backup start");
        } else {
            if (range->end_lsn != tlep[i]->end)
                ereport(ERROR, "manifest WAL end doesn't match timeline switch");
        }
    }

    // Wait for WAL summarization to catch up
    WaitForWalSummarization(backup_state->startpoint);

    // Get all WAL summaries for the required LSN range
    all_wslist = GetWalSummaries(0, earliest_wal_range_start_lsn, backup_state->startpoint);

    // Collect required WAL summaries for each timeline
    foreach(lc, expectedTLEs) {
        TimeLineHistoryEntry *tle = lfirst(lc);
        XLogRecPtr tli_start_lsn = tle->begin;
        XLogRecPtr tli_end_lsn = tle->end;
        XLogRecPtr tli_missing_lsn;
        List *tli_wslist;

        // Skip timelines after backup start
        if (tle->tli == backup_state->starttli) {
            found_backup_start_tli = true;
            tli_end_lsn = backup_state->startpoint;
        } else if (!found_backup_start_tli) {
            continue;
        }

        // Adjust start LSN for earliest timeline
        if (tle->tli == earliest_wal_range_tli)
            tli_start_lsn = earliest_wal_range_start_lsn;

        // Filter summaries for this timeline and LSN range
        tli_wslist = FilterWalSummaries(all_wslist, tle->tli, tli_start_lsn, tli_end_lsn);

        // Verify summaries are complete
        if (!WalSummariesAreComplete(tli_wslist, tli_start_lsn, tli_end_lsn, &tli_missing_lsn)) {
            if (XLogRecPtrIsInvalid(tli_missing_lsn))
                ereport(ERROR, "no WAL summaries exist for required range");
            else
                ereport(ERROR, "WAL summaries incomplete for required range");
        }

        // Add to list of summaries to read
        required_wslist = list_concat(required_wslist, tli_wslist);

        // Stop at earliest timeline
        if (tle->tli == earliest_wal_range_tli)
            break;
    }

    // Create and populate block reference table from WAL summaries
    ib->brtab = CreateEmptyBlockRefTable();
    foreach(lc, required_wslist) {
        WalSummaryFile *ws = lfirst(lc);
        WalSummaryIO wsio;
        BlockRefTableReader *reader;
        RelFileLocator rlocator;
        ForkNumber forknum;
        BlockNumber limit_block;
        BlockNumber blocks[BLOCKS_PER_READ];

        // Open and read WAL summary file
        wsio.file = OpenWalSummaryFile(ws, false);
        wsio.filepos = 0;
        reader = CreateBlockRefTableReader(ReadWalSummary, &wsio,
                                         FilePathName(wsio.file),
                                         ReportWalSummaryError, NULL);

        // Process each relation in the summary
        while (BlockRefTableReaderNextRelation(reader, &rlocator, &forknum, &limit_block)) {
            BlockRefTableSetLimitBlock(ib->brtab, &rlocator, forknum, limit_block);

            // Read and mark all modified blocks
            while (1) {
                unsigned nblocks = BlockRefTableReaderGetBlocks(reader, blocks, BLOCKS_PER_READ);
                if (nblocks == 0) break;

                for (unsigned i = 0; i < nblocks; ++i)
                    BlockRefTableMarkBlockModified(ib->brtab, &rlocator, forknum, blocks[i]);
            }
        }
        DestroyBlockRefTableReader(reader);
        FileClose(wsio.file);
    }

    // Restore previous memory context
    MemoryContextSwitchTo(oldcontext);
}
```

Key simplifications made:
- Removed verbose error message formatting for clarity
- Simplified complex timeline ordering logic with clearer variable names
- Consolidated similar error checking patterns
- Removed detailed comments explaining edge cases
- Abstracted low-level file I/O details while preserving core algorithm
- Combined related validation steps into logical blocks
- Simplified loop structures for readability