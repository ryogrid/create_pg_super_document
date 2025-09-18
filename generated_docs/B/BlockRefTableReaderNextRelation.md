# BlockRefTableReaderNextRelation

## Location
src/common/blkreftable.c: 613 - 688

## Overview
Reads the next relation fork entry from a block reference table file, advancing the reader to the next relation and preparing it for block data extraction.

## Definition


## Detailed Description
BlockRefTableReaderNextRelation sequentially processes entries in a serialized block reference table file, extracting metadata for the next relation fork. The function reads a serialized entry, checks for the end-of-file sentinel (all zeros), and if found, validates the file's CRC checksum for integrity verification. For valid entries, it allocates and reads the chunk size array, sets up internal state for subsequent block data reading, and returns the relation information to the caller. The function enforces proper usage by requiring all chunks from the previous relation to be consumed before advancing.

## Parameters / Member Variables
- : Pointer to the BlockRefTableReader maintaining the current read state
- : Output parameter receiving the RelFileLocator for the next relation
- : Output parameter receiving the fork number (main, FSM, VM, etc.)
- : Output parameter receiving the highest block number referenced in this relation

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