# tarPaddingBytesRequired

## Location
[src/include/pgtar.h:79-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgtar.h#L79-L84)

## Overview
Computes the number of padding bytes required for an entry in a tar archive to align it to a multiple of TAR_BLOCK_SIZE.

## Definition


## Detailed Description
This function calculates the padding bytes needed to align a tar archive entry to the required block boundary. TAR format requires all entries to be aligned to TAR_BLOCK_SIZE boundaries (typically 512 bytes). The function uses the TYPEALIGN macro to efficiently compute the aligned size and returns the difference between the aligned size and the original length, which represents the number of padding bytes needed.

Since TAR_BLOCK_SIZE is a power of 2, the TYPEALIGN macro can use bitwise operations for efficient alignment calculation.

## Parameters / Member Variables
- : The current length of the data that needs to be padded to align to TAR_BLOCK_SIZE boundary

## Dependencies
- Functions called/Symbols referenced:
  - TYPEALIGN
  - TAR_BLOCK_SIZE
- Called from (representative examples):
  - [sendDir](../s/sendDir.md) (src/backend/backup/basebackup.c:1536)
  - [_tarWritePadding](_tarWritePadding.md) (src/backend/backup/basebackup.c:2073)
  - [bbstreamer_tar_header](../b/bbstreamer_tar_header.md) (src/bin/pg_basebackup/bbstreamer_tar.c:305)
  - [bbstreamer_tar_archiver_content](../b/bbstreamer_tar_archiver_content.md) (src/bin/pg_basebackup/bbstreamer_tar.c:417)
  - tar_close (src/bin/pg_basebackup/walmethods.c:1114)
  - [_tarAddFile](_tarAddFile.md) (src/bin/pg_dump/pg_backup_tar.c:1054)
  - [_tarPositionTo](_tarPositionTo.md) (src/bin/pg_dump/pg_backup_tar.c:1121)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Used throughout PostgreSQL's tar-related functionality including base backups, WAL methods, and pg_dump
- The calculation ensures proper tar archive format compliance by maintaining block alignment
- Returns 0 if the input length is already aligned to TAR_BLOCK_SIZE