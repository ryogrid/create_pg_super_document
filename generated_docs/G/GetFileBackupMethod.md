# GetFileBackupMethod

## Location
[src/backend/backup/basebackup_incremental.c:667-870](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_incremental.c#L667-L870)

## Overview
Determines whether a file should be backed up fully or incrementally by analyzing block-level changes recorded in WAL summaries since the prior backup.

## Definition


## Detailed Description
This is the core decision-making function for incremental backups, determining how each database file should be handled. It performs a sophisticated analysis based on the block reference table built by PrepareForIncrementalBackup to decide whether to:

1. **Back up the entire file** (BACK_UP_FILE_FULLY) - when most blocks have changed, the file is new, or special conditions apply
2. **Back up incrementally** (BACK_UP_FILE_INCREMENTALLY) - when only a subset of blocks have changed, providing the specific blocks needed

The function's decision process includes:

- **Validation**: Ensures file size is valid and aligned to block boundaries
- **Fork-specific logic**: Free-space maps are always backed up fully due to incomplete WAL logging
- **File existence checks**: New files since the prior backup are backed up fully
- **Database creation detection**: Files in newly created databases are backed up fully  
- **Block change analysis**: Uses the block reference table to identify which specific blocks have been modified
- **Efficiency thresholds**: Falls back to full backup when >90% of blocks would be included anyway
- **Block number processing**: Sorts and converts absolute block numbers to relative offsets for the specific segment

For incremental backups, the function outputs the exact list of blocks that need to be included and calculates the truncation length for proper reconstruction.

## Parameters / Member Variables
- : IncrementalBackupInfo containing the block reference table and manifest data
- : File system path to the file being analyzed
- : Database OID (may be InvalidOid for shared relations)
- : Tablespace OID
- : Relation file number
- : Fork number (main, fsm, vm, etc.)
- : Segment number for large relations
- : Current size of the file in bytes
- : Output parameter for number of blocks needed in incremental backup
- : Output array of block numbers (relative to segment) to include
- : Output parameter for minimum reconstructed file length

## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid, RelFileNumberIsValid: Parameter validation
  - backup_file_lookup: Checks if file existed in prior backup
  - [GetIncrementalFilePath](GetIncrementalFilePath.md): Gets incremental file path for lookups
  - BlockRefTableGetEntry: Looks up change information for relation
  - BlockRefTableEntryGetBlocks: Gets list of modified blocks
  - qsort, compare_block_numbers: Sorts block numbers
  - BlockNumberIsValid: Validates limit block
- Constants referenced:
  - BLCKSZ, RELSEG_SIZE: Block and segment size constants
  - FSM_FORKNUM, MAIN_FORKNUM: Fork number constants
  - BACK_UP_FILE_FULLY, BACK_UP_FILE_INCREMENTALLY: Return values
- Types referenced:
  - [IncrementalBackupInfo](../I/IncrementalBackupInfo.md), FileBackupMethod, BlockRefTableEntry
- Called from:
  - [sendDir](../s/sendDir.md) (src/backend/backup/basebackup.c:1502)

## Notes and Other Information
- Must be called after PrepareForIncrementalBackup has completed
- The 90% threshold for falling back to full backup is a performance optimization that could potentially be made configurable
- Special handling for zero-length files avoids creating incremental files larger than full backups
- Block number arrays are sorted to optimize subsequent processing during backup restoration
- The truncation length calculation ensures proper reconstruction by indicating the minimum file size required
- Handles complex edge cases like files being deleted and recreated between backups
- The function assumes that WAL replay during recovery will handle any inconsistencies from files that were modified after backup start