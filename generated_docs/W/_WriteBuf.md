# _WriteBuf

## Location
src/bin/pg_dump/pg_backup_custom.c: 704 - 717

## Overview
Writes a buffer of data to the PostgreSQL custom format archive file with comprehensive error handling.

## Definition
```c
static void _WriteBuf(ArchiveHandle *AH, const void *buf, size_t len)
```

## Detailed Description
This function is a fundamental building block for writing bulk data to PostgreSQL custom format archives. It provides an efficient interface for writing blocks of bytes to the archive file. The function is marked as "Mandatory" in the comments, indicating it is a required implementation for the custom archive format.

The function serves as a wrapper around the standard library fwrite() function, adding error handling specific to the archive writing context. It ensures that the entire buffer is written successfully, treating partial writes as error conditions.

## Parameters / Member Variables
- `AH`: Archive handle containing the file handle and archive context for writing operations
- `buf`: Pointer to the data buffer to be written to the archive
- `len`: Number of bytes to write from the buffer

## Dependencies
- Functions called/Symbols referenced:
  - WRITE_ERROR_EXIT: Error handling macro for write failures
- Called from (representative examples):
  - InitArchiveFmt_Custom: Used during custom format archive initialization
  - _CustomWriteFunc: Used for custom format-specific write operations
  - lclTocEntry: Used in directory format implementation
  - InitArchiveFmt_Directory: Used in directory format initialization
  - InitArchiveFmt_Null: Used in null format initialization

## Notes and Other Information
- This is a static function specific to the custom format archive handling
- No return value - exits on error via WRITE_ERROR_EXIT macro
- Part of the mandatory interface that archive formats must implement
- Ensures complete buffer writes - partial writes are treated as errors
- Used extensively throughout the pg_dump/pg_restore infrastructure for bulk data writing
- More efficient than _WriteByte for writing larger blocks of data
- Complementary to buffer reading operations in the archive system
- File location: src/bin/pg_dump/pg_backup_custom.c:704-717