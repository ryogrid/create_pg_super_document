# _CustomReadFunc

## Location
[src/bin/pg_dump/pg_backup_custom.c:1003-1024](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L1003-L1024)

## Overview
A callback function used by the PostgreSQL pg_dump custom format archive reader to read compressed data blocks from archive files.

## Definition


## Detailed Description
The  function serves as a callback for the archive reading mechanism in PostgreSQL's custom backup format. It implements a simple reading strategy where one compressed block is read at a time. The function is designed to work with the compression system, providing data to decompressors during the restoration process.

The function follows a two-step process: first reading the length of the compressed block from the archive, then reading the actual data block. It dynamically manages memory allocation for the buffer, ensuring it's large enough to hold the incoming data block.

## Parameters / Member Variables
- : Archive handle containing file handle and archive metadata
- : Pointer to buffer pointer that will hold the read data (may be reallocated)
- : Pointer to current buffer length (updated if buffer is reallocated)

## Dependencies
- Functions called/Symbols referenced:
  - [ReadInt](../R/ReadInt.md) (reads the block length from archive)
  - pg_malloc (allocates memory for larger buffers)
  - [_ReadBuf](../R/_ReadBuf.md) (reads the actual data block from archive)
- Called from (representative examples):
  - lclTocEntry (function pointer assignment at line 92)
  - [_PrintData](../P/_PrintData.md) (passed to AllocateCompressor at line 574)

## Notes and Other Information
- This is a static function internal to the pg_backup_custom.c module
- The function exits the application on read errors through _ReadBuf
- Memory management is handled automatically - the function will free and reallocate the buffer if it's too small
- Returns 0 when no more data is available (blkLen == 0)
- Used specifically with the custom format archive type in pg_dump/pg_restore
- Works in conjunction with PostgreSQL's compression system for efficient data storage and retrieval