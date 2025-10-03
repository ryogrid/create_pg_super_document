# BlockRefTableReaderNextRelation

## Location
[src/common/blkreftable.c:613-688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L613-L688)

## Overview
Reads the next relation fork entry from a block reference table file, advancing the reader to the next relation and preparing it for block data extraction.

## Definition

```c
bool
BlockRefTableReaderNextRelation(BlockRefTableReader *reader,
								RelFileLocator *rlocator,
								ForkNumber *forknum,
								BlockNumber *limit_block)
```
## Detailed Description
BlockRefTableReaderNextRelation sequentially processes entries in a serialized block reference table file, extracting metadata for the next relation fork. The function reads a serialized entry, checks for the end-of-file sentinel (all zeros), and if found, validates the file's CRC checksum for integrity verification. For valid entries, it allocates and reads the chunk size array, sets up internal state for subsequent block data reading, and returns the relation information to the caller. The function enforces proper usage by requiring all chunks from the previous relation to be consumed before advancing.

## Parameters / Member Variables
- `*reader`: Pointer to the BlockRefTableReader maintaining the current read state
- `*rlocator`: Output parameter receiving the RelFileLocator for the next relation
- `*forknum`: Output parameter receiving the fork number (main, FSM, VM, etc.)
- `*limit_block`: Output parameter receiving the highest block number referenced in this relation
## Dependencies
- Functions called/Symbols referenced:
  - [BlockRefTableRead](BlockRefTableRead.md)
  - FIN_CRC32C
  - EQ_CRC32C
  - [palloc](../p/palloc.md)
  - [pfree](../p/pfree.md)
  - memcmp
  - memcpy
- Called from (representative examples):
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md)
  - [pg_wal_summary_contents](../p/pg_wal_summary_contents.md)

## Notes and Other Information
- Returns false when reaching end of file (sentinel entry), true for valid entries
- Enforces sequential reading pattern: all blocks must be consumed before advancing to next relation
- Validates file integrity by checking CRC32C checksum when reaching end of file
- Manages memory for chunk size arrays, freeing previous allocation before reading new data
- Uses zero-filled entry as sentinel to detect end-of-file condition
- CRC calculation excludes the 4-byte CRC value itself to maintain consistency
- Caller must call BlockRefTableReaderGetBlocks until it returns 0 before calling this function again

## Simplified Source

```c
bool
BlockRefTableReaderNextRelation(BlockRefTableReader *reader,
                               RelFileLocator *rlocator,
                               ForkNumber *forknum,
                               BlockNumber *limit_block)
{
    BlockRefTableSerializedEntry sentry;
    BlockRefTableSerializedEntry zentry = {{0}};  // Sentinel for end-of-file

    // Ensure all chunks from previous relation were consumed
    Assert(reader->total_chunks == reader->consumed_chunks);

    // Read next serialized entry from file
    BlockRefTableRead(reader, &sentry,
                     sizeof(BlockRefTableSerializedEntry));

    // Check if we've reached the end-of-file sentinel
    if (memcmp(&sentry, &zentry, sizeof(BlockRefTableSerializedEntry)) == 0)
    {
        pg_crc32c expected_crc;
        pg_crc32c actual_crc;

        // Calculate expected CRC (excluding the CRC value itself)
        expected_crc = reader->buffer.crc;
        FIN_CRC32C(expected_crc);

        // Read actual CRC from file
        BlockRefTableRead(reader, &actual_crc, sizeof(pg_crc32c));

        // Verify file integrity
        if (!EQ_CRC32C(expected_crc, actual_crc))
            reader->error_callback(reader->error_callback_arg,
                                  "file \"%s\" has wrong checksum: expected %08X, found %08X",
                                  reader->error_filename, expected_crc, actual_crc);

        return false;  // End of file reached
    }

    // Read chunk size array for this relation
    if (reader->chunk_size != NULL)
        pfree(reader->chunk_size);
    reader->chunk_size = palloc(sentry.nchunks * sizeof(uint16));
    BlockRefTableRead(reader, reader->chunk_size,
                     sentry.nchunks * sizeof(uint16));

    // Set up state for reading chunks
    reader->total_chunks = sentry.nchunks;
    reader->consumed_chunks = 0;

    // Return relation information to caller
    memcpy(rlocator, &sentry.rlocator, sizeof(RelFileLocator));
    *forknum = sentry.forknum;
    *limit_block = sentry.limit_block;
    return true;
}
```