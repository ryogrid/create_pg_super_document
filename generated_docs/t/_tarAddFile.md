# _tarAddFile

## Location
[src/bin/pg_dump/pg_backup_tar.c:1014-1065](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_tar.c#L1014-L1065)

## Overview
A static function that adds a file to a TAR archive by writing the TAR header followed by the file content, with proper padding to TAR block boundaries.

## Definition
```c
static void _tarAddFile(ArchiveHandle *AH, TAR_MEMBER *th)
```

## Detailed Description
This function implements the core functionality for adding files to TAR archives in pg_dump. It first determines the file length by seeking to the end of the temporary file, then writes the TAR header using _tarWriteHeader(). The file content is copied in 32KB chunks from the temporary file to the TAR archive. After copying, it ensures the file length matches expectations and adds null-byte padding to align with TAR block boundaries (512-byte blocks). The function also updates the TAR file position counter and cleans up by closing the temporary file.

## Parameters / Member Variables
- `AH`: Archive handle containing output stream and format-specific data
- `th`: TAR_MEMBER structure containing file information and temporary file handle

## Dependencies
- Functions called/Symbols referenced:
  - fseeko/ftello (file positioning)
  - [_tarWriteHeader](_tarWriteHeader.md) (writes TAR header block)  
  - fread/fwrite (file I/O operations)
  - [tarPaddingBytesRequired](tarPaddingBytesRequired.md) (calculates padding needed)
  - [lclContext](../l/lclContext.md) (local context struct type)
  - pgoff_t (PostgreSQL offset type)
  - WRITE_ERROR_EXIT/READ_ERROR_EXIT (error handling macros)
- Called from (representative examples):
  - [tarClose](tarClose.md) (during archive finalization)

## Notes and Other Information
- Uses a 32KB buffer (32768 bytes) for efficient file copying
- Enforces TAR format requirement of 512-byte block alignment with null padding
- Performs length validation to ensure data integrity
- Automatically deletes the temporary file after processing
- Updates the TAR file position counter to track archive size
- Part of the TAR archive creation functionality in pg_dump
- Located in src/bin/pg_dump/pg_backup_tar.c:1014-1065