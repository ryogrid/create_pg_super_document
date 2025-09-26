# BlockRefTableReader

## Location
[src/common/blkreftable.c:200-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L200-L216)

## Overview
The `BlockRefTableReader` struct maintains state for incrementally reading block reference table files from disk, tracking progress through chunks of block information for RelFileLocator/ForkNumber combinations.

## Definition
```c
struct BlockRefTableReader
{
    BlockRefTableBuffer buffer;
    char           *error_filename;
    report_error_fn error_callback;
    void           *error_callback_arg;
    uint32          total_chunks;
    uint32          consumed_chunks;
    uint16         *chunk_size;
    uint16          chunk_data[MAX_ENTRIES_PER_CHUNK];
    uint32          chunk_position;
};
```

## Detailed Description
The `BlockRefTableReader` is used to process block reference table files incrementally, maintaining state across multiple read operations. It reads chunk-based data for specific RelFileLocator/ForkNumber combinations and tracks progress through both chunks and individual blocks within chunks. The reader supports bitmap and entry-based chunks, using `chunk_position` to track scanning progress differently for each format.

## Parameters / Member Variables
- `buffer`: BlockRefTableBuffer instance for file I/O operations with buffering and CRC checking
- `error_filename`: Name of the file being processed for error reporting purposes
- `error_callback`: Function pointer for custom error handling during read operations
- `error_callback_arg`: User-defined argument passed to the error callback function
- `total_chunks`: Total number of chunks for the current RelFileLocator/ForkNumber combination
- `consumed_chunks`: Number of chunks that have been completely read and processed
- `chunk_size`: Array containing the size of each chunk (length equals total_chunks)
- `chunk_data`: Buffer holding data for the currently loaded chunk (up to MAX_ENTRIES_PER_CHUNK entries)
- `chunk_position`: Current position within the active chunk - number of bits scanned for bitmaps or entries scanned for other chunks

## Dependencies
- Functions called/Symbols referenced:
  - BlockRefTableBuffer
  - MAX_ENTRIES_PER_CHUNK
- Called from (representative examples):
  - CreateBlockRefTableReader
  - BlockRefTableReaderNextRelation
  - BlockRefTableReaderGetBlocks
  - DestroyBlockRefTableReader
  - PrepareForIncrementalBackup
  - pg_wal_summary_contents

## Notes and Other Information
- The reader processes chunks atomically - a chunk is either completely read or not read at all
- Supports both bitmap-based and entry-based chunk formats with different position tracking
- Used extensively in incremental backup operations and WAL summary processing
- Part of PostgreSQL's block reference table infrastructure for tracking database file changes