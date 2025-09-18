# _CustomWriteFunc

## Location
src/bin/pg_dump/pg_backup_custom.c: 988 - 1002

## Overview
This function serves as a callback for the compression subsystem to write compressed data blocks to the custom-format archive file.

## Definition
```c
static void _CustomWriteFunc(ArchiveHandle *AH, const char *buf, size_t len)
```

## Detailed Description
_CustomWriteFunc is a callback function used by the compression system when writing compressed data to custom-format archives. It follows the standard write callback interface expected by AllocateCompressor(). The function writes data blocks in a specific format: first it writes the length of the data block as an integer, followed by the actual data buffer. This length-prefixed format allows the reader to know exactly how many bytes to read for each compressed block. The function includes a safety check to avoid writing zero-length blocks, though such blocks should not occur in normal operation.

## Parameters / Member Variables
- `AH`: Archive handle containing file handle and other archive state information
- `buf`: Buffer containing the compressed data to be written to the archive
- `len`: Length of the data in the buffer (in bytes)

## Dependencies
- Functions called/Symbols referenced:
  - WriteInt (function to write integer values to archive)
  - _WriteBuf (function to write raw buffer data to archive)
- Called from (representative examples):
  - AllocateCompressor (through callback mechanism in _StartData and _StartLO)

## Notes and Other Information
- This is a static function internal to pg_backup_custom.c
- Used as a callback function pointer passed to AllocateCompressor()
- Implements a length-prefixed data block format (length followed by data)
- Includes a safety check to prevent writing zero-length blocks
- Part of the custom archive format's compressed data handling mechanism
- The function is called by the compression subsystem whenever it has compressed data ready to write
- The length-prefixed format enables efficient reading during restore operations
- Works in conjunction with compression algorithms to store compressed table and large object data