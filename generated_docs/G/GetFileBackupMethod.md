# GetFileBackupMethod

## Location
[src/backend/backup/basebackup_incremental.c:667-870](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_incremental.c#L667-L870)

## Overview
Determines whether a file should be backed up fully or incrementally by analyzing block-level changes recorded in WAL summaries since the prior backup.

## Definition

```c
FileBackupMethod
GetFileBackupMethod(IncrementalBackupInfo *ib, const char *path,
					Oid dboid, Oid spcoid,
					RelFileNumber relfilenumber, ForkNumber forknum,
					unsigned segno, size_t size,
					unsigned *num_blocks_required,
					BlockNumber *relative_block_numbers,
					unsigned *truncation_block_length)
```
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
- `*ib`: IncrementalBackupInfo containing the block reference table and manifest data
- `*path`: File system path to the file being analyzed
- `dboid`: Database OID (may be InvalidOid for shared relations)
- `spcoid`: Tablespace OID
- `relfilenumber`: Relation file number
- `forknum`: Fork number (main, fsm, vm, etc.)
- `segno`: Segment number for large relations
- `size`: Current size of the file in bytes
- `*num_blocks_required`: Output parameter for number of blocks needed in incremental backup
- `*relative_block_numbers`: Output array of block numbers (relative to segment) to include
- `*truncation_block_length`: Output parameter for minimum reconstructed file length
## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid, RelFileNumberIsValid: Parameter validation
  - backup_file_lookup: Checks if file existed in prior backup
  - [GetIncrementalFilePath](GetIncrementalFilePath.md): Gets incremental file path for lookups
  - [BlockRefTableGetEntry](../B/BlockRefTableGetEntry.md): Looks up change information for relation
  - [BlockRefTableEntryGetBlocks](../B/BlockRefTableEntryGetBlocks.md): Gets list of modified blocks
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

## Simplified Source

```c
// Determine how to backup a file: fully or incrementally with specific blocks
FileBackupMethod GetFileBackupMethod(IncrementalBackupInfo *ib, const char *path,
                                     Oid dboid, Oid spcoid, RelFileNumber relfilenumber,
                                     ForkNumber forknum, unsigned segno, size_t size,
                                     unsigned *num_blocks_required,
                                     BlockNumber *relative_block_numbers,
                                     unsigned *truncation_block_length)
{
    BlockNumber limit_block, start_blkno, stop_blkno;
    RelFileLocator rlocator;
    BlockRefTableEntry *brtentry;
    unsigned nblocks;

    // Validate that we're in incremental backup mode
    Assert(ib->buf.data == NULL);
    Assert(OidIsValid(spcoid) && RelFileNumberIsValid(relfilenumber));

    // If file size is invalid or not block-aligned, backup fully
    if ((size % BLCKSZ) != 0 || size / BLCKSZ > RELSEG_SIZE)
        return BACK_UP_FILE_FULLY;

    // Free-space maps aren't properly WAL-logged, always backup fully
    if (forknum == FSM_FORKNUM)
        return BACK_UP_FILE_FULLY;

    // Check if file existed in prior backup
    if (backup_file_lookup(ib->manifest_files, path) == NULL) {
        char *ipath = GetIncrementalFilePath(dboid, spcoid, relfilenumber, forknum, segno);
        if (backup_file_lookup(ib->manifest_files, ipath) == NULL)
            return BACK_UP_FILE_FULLY;
    }

    // Check if entire database was created since last backup
    rlocator.spcOid = spcoid;
    rlocator.dbOid = dboid;
    rlocator.relNumber = 0;
    if (BlockRefTableGetEntry(ib->brtab, &rlocator, MAIN_FORKNUM, &limit_block) != NULL)
        return BACK_UP_FILE_FULLY;

    // Look up block reference table entry for this specific relation
    rlocator.relNumber = relfilenumber;
    brtentry = BlockRefTableGetEntry(ib->brtab, &rlocator, forknum, &limit_block);

    // If no entry exists, no WAL-logged changes occurred
    if (brtentry == NULL) {
        if (size == 0)  // Empty files should be backed up fully
            return BACK_UP_FILE_FULLY;
        *num_blocks_required = 0;
        *truncation_block_length = size / BLCKSZ;
        return BACK_UP_FILE_INCREMENTALLY;
    }

    // If limit block is before this segment, backup fully
    if (limit_block <= segno * RELSEG_SIZE)
        return BACK_UP_FILE_FULLY;

    // Calculate block number range for this segment
    start_blkno = segno * RELSEG_SIZE;
    stop_blkno = start_blkno + (size / BLCKSZ);

    // Detect overflow in block number calculations
    if (start_blkno / RELSEG_SIZE != segno || stop_blkno < start_blkno)
        ereport(ERROR, ...);

    // Get list of modified blocks in this segment
    nblocks = BlockRefTableEntryGetBlocks(brtentry, start_blkno, stop_blkno,
                                          relative_block_numbers, RELSEG_SIZE);

    // If >90% of blocks changed, backup fully (optimization threshold)
    if (nblocks * BLCKSZ > size * 0.9)
        return BACK_UP_FILE_FULLY;

    // Sort block numbers and convert to relative offsets
    qsort(relative_block_numbers, nblocks, sizeof(BlockNumber), compare_block_numbers);
    if (start_blkno != 0) {
        for (unsigned i = 0; i < nblocks; ++i)
            relative_block_numbers[i] -= start_blkno;
    }

    *num_blocks_required = nblocks;

    // Calculate truncation length for reconstruction
    *truncation_block_length = size / BLCKSZ;
    if (BlockNumberIsValid(limit_block)) {
        unsigned relative_limit = limit_block - segno * RELSEG_SIZE;
        if (*truncation_block_length < relative_limit)
            *truncation_block_length = relative_limit;
    }

    return BACK_UP_FILE_INCREMENTALLY;
}
```

**Key Points:**
- Decides between full backup vs incremental backup with specific blocks
- Validates file properties and handles special cases (FSM forks, new files)
- Uses block reference table to find which blocks changed since last backup
- Falls back to full backup when >90% of blocks would be included
- Outputs sorted list of relative block numbers for incremental backup
- Calculates truncation length for proper file reconstruction