# process_target_wal_block_change

## Location
[src/bin/pg_rewind/filemap.c:352-408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L352-L408)

## Overview
A callback function that processes WAL block changes in the target system during pg_rewind, determining which data blocks need to be overwritten from the source system and recording them in a bitmap.

## Definition
void process_target_wal_block_change(ForkNumber forknum, RelFileLocator rlocator, BlockNumber blkno)

## Detailed Description
This function is called during WAL processing in the target system for every block that has been changed. It analyzes whether a specific block in a relation file needs to be replaced with the corresponding block from the source system during the rewind operation. The function makes this determination based on the existence and size of files in both source and target systems.

The function implements sophisticated logic to handle various scenarios:
1. **Both files exist**: Marks blocks for overwrite if they exist within the bounds of both source and target files
2. **Missing files**: Safely ignores blocks from files that don't exist in either system or will be truncated/removed anyway
3. **File validation**: Ensures that page modifications only occur on regular files, not directories or symlinks
4. **Segment handling**: Properly handles PostgreSQL's file segmentation by calculating the correct segment and block offset

The function is a critical component of pg_rewind's incremental synchronization approach, ensuring that only the necessary data blocks are copied rather than entire files.

## Parameters / Member Variables
- `forknum`: The fork number identifying which fork of the relation (main, FSM, visibility map, etc.)
- `rlocator`: The RelFileLocator structure containing tablespace, database, and relation identifiers
- `blkno`: The block number within the relation that was changed

## Dependencies
- Functions called/Symbols referenced:
  - [file_entry_t](../f/file_entry_t.md) (structure type)
  - [datasegpath](../d/datasegpath.md) (function to construct data segment file path)
  - [lookup_filehash_entry](../l/lookup_filehash_entry.md) (function to find file entry in hash table)
  - [pfree](pfree.md) (memory deallocation function)
  - Assert (assertion macro)
  - FILE_TYPE_REGULAR (file type enum value)
  - [pg_fatal](pg_fatal.md) (error reporting function)
  - [datapagemap_add](../d/datapagemap_add.md) (function to add block to bitmap)
  - RELSEG_SIZE (constant for relation segment size)
  - BLCKSZ (constant for block size)
- Called from (representative examples):
  - [extractPageInfo](../e/extractPageInfo.md) (in parsexlog.c:481)

## Notes and Other Information
- This function requires that all files in both source and target systems have already been processed and added to the filehash table
- Uses PostgreSQL's relation file segmentation: calculates segment number and block offset within segment
- Only processes blocks that exist within the bounds of both source and target files to avoid unnecessary work
- Safely handles cases where relations were dropped in either or both systems
- The target_pages_to_overwrite bitmap is used later during the actual file copying phase
- Contains detailed comments explaining the logic for ignoring certain blocks (beyond EOF, non-existent files, etc.)
- Part of the WAL analysis phase that determines the minimal set of changes needed for rewind
- Critical for incremental rewind performance: avoids copying entire files when only specific blocks need updating

## Simplified Source

```c
void process_target_wal_block_change(ForkNumber forknum, RelFileLocator rlocator,
                                    BlockNumber blkno)
{
    char *file_path;
    file_entry_t *file_entry;
    BlockNumber block_in_segment;
    int segment_number;

    // Calculate which segment file contains this block
    segment_number = blkno / RELSEG_SIZE;
    block_in_segment = blkno % RELSEG_SIZE;

    // Get the path to the data segment file
    file_path = datasegpath(rlocator, forknum, segment_number);
    file_entry = lookup_filehash_entry(file_path);
    pfree(file_path);

    // Only process if file entry exists in our hash table
    if (file_entry)
    {
        Assert(file_entry->isrelfile);

        // Check if file exists in target system
        if (file_entry->target_exists)
        {
            // Ensure we're dealing with a regular file
            if (file_entry->target_type != FILE_TYPE_REGULAR)
                pg_fatal("unexpected page modification for non-regular file \"%s\"",
                        file_entry->path);

            // If file also exists in source system, consider copying this block
            if (file_entry->source_exists)
            {
                off_t block_end_offset = (block_in_segment + 1) * BLCKSZ;

                // Only mark block for overwrite if it exists in both files
                if (block_end_offset <= file_entry->source_size &&
                    block_end_offset <= file_entry->target_size)
                {
                    datapagemap_add(&file_entry->target_pages_to_overwrite,
                                   block_in_segment);
                }
            }
        }
    }

    /*
     * If file doesn't exist in our hash table, or doesn't exist in either
     * source or target, we can safely ignore this WAL record. The file
     * will be handled appropriately during the copy phase.
     */
}
```