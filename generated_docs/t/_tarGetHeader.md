# _tarGetHeader

## Location
[src/bin/pg_dump/pg_backup_tar.c:1140-1211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_tar.c#L1140-L1211)

## Overview
Reads and verifies a TAR header block from a tar archive file during PostgreSQL pg_dump restore operations.

## Definition
```c
static int _tarGetHeader(ArchiveHandle *AH, TAR_MEMBER *th)
```

## Detailed Description
This function is a core component of PostgreSQL's TAR archive handling in pg_dump/pg_restore operations. It reads a TAR_BLOCK_SIZE (512 bytes) header block from the archive file, validates its integrity using checksum verification, and extracts essential file information including filename and size.

The function implements robust error handling by:
- Reading raw TAR blocks and handling EOF conditions gracefully
- Verifying TAR header checksums to detect corruption
- Skipping null blocks (empty padding blocks in TAR format)
- Extracting and null-terminating filenames from the TAR header
- Providing detailed logging and error reporting

The function follows the POSIX TAR format specification, parsing standard TAR header fields at specific byte offsets within the 512-byte header block.

## Parameters / Member Variables
- `AH`: Archive handle containing the format-specific data and file handles for the TAR archive
- `th`: TAR_MEMBER structure that will be populated with the extracted header information (filename and file length)

## Dependencies
- Functions called/Symbols referenced:
  - [_tarReadRaw](_tarReadRaw.md)
  - [tarChecksum](tarChecksum.md)
  - [read_tar_number](../r/read_tar_number.md)
  - strlcpy
  - pg_log_debug
  - [pg_fatal](../p/pg_fatal.md)
  - ngettext
  - ftello
- Called from (representative examples):
  - [_tarPositionTo](_tarPositionTo.md)

## Notes and Other Information
- Returns 1 on successful header read, 0 on EOF
- The function handles TAR format specifics like 100-byte filename fields that may not be null-terminated
- Implements checksum validation according to TAR format standards
- Uses pgoff_t for file position and size handling to support large files
- Provides multilingual error messages using ngettext for proper pluralization
- File position tracking is maintained for debugging and error reporting purposes