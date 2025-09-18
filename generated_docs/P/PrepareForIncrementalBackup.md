# PrepareForIncrementalBackup

## Location
src/backend/backup/basebackup_incremental.c: 265 - 626

## Overview
Validates manifest data and prepares the WAL summary infrastructure required to perform an incremental backup by analyzing timeline history, verifying WAL ranges, and building an in-memory block reference table.

## Definition


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
  - readTimeLineHistory: Reads timeline history for validation
  - WaitForWalSummarization: Ensures WAL summarization is caught up
  - GetWalSummaries, FilterWalSummaries: Retrieves and filters WAL summary files
  - WalSummariesAreComplete: Validates completeness of WAL summaries
  - CreateEmptyBlockRefTable: Initializes block reference table
  - OpenWalSummaryFile, CreateBlockRefTableReader: Reads WAL summary files
  - BlockRefTableReaderNextRelation, BlockRefTableReaderGetBlocks: Parses summary data
  - BlockRefTableSetLimitBlock, BlockRefTableMarkBlockModified: Builds block reference table
- Types referenced:
  - IncrementalBackupInfo, BackupState, TimeLineHistoryEntry
  - backup_wal_range, WalSummaryFile, WalSummaryIO, BlockRefTableReader
- Called from:
  - perform_base_backup (src/backend/backup/basebackup.c:287)

## Notes and Other Information
- This function performs extensive error checking and will throw detailed error messages if the manifest is inconsistent with the server's timeline history
- The function handles complex timeline scenarios including timeline switches that may have occurred during the prior backup
- Memory management is handled through the IncrementalBackupInfo's memory context
- The block reference table built by this function can be quite large, as noted in the IncrementalBackupInfo structure comments
- WAL summarization must be enabled and functional for this function to succeed
- The function provides helpful hints for common issues like rate-limiting on standby servers