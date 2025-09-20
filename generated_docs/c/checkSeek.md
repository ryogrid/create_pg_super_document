# checkSeek

## Location
[src/bin/pg_dump/pg_backup_archiver.c:4111-4135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L4111-L4135)

## Overview
Tests whether a file pointer supports seek operations (ftello/fseeko) to determine if random access is possible on the given file stream.

## Definition

```c
bool
checkSeek(FILE *fp)
```
## Detailed Description
This utility function determines whether a file stream supports seeking operations, which is essential for PostgreSQL archive formats that require random access capabilities. The function performs two critical tests:

1. **Position query test**: Uses ftello() to get the current file position, ensuring the stream supports position tracking
2. **Seek operation test**: Uses fseeko() with SEEK_SET to verify actual seeking capability

The function employs a robust testing strategy by:
- Getting the current position first to have a valid target for the seek test
- Using SEEK_SET with the current position rather than SEEK_CUR with offset 0, because some platforms incorrectly report success for SEEK_CUR operations on unseekable streams
- Returning boolean status to allow callers to gracefully handle unseekable streams

This is particularly important for PostgreSQL archives since some operations require random access to different parts of the archive file.

## Parameters / Member Variables
- : FILE pointer to test for seek capability

## Dependencies
- Functions called/Symbols referenced:
  - pgoff_t (PostgreSQL offset type for large file support)
  - ftello (get current file position with large file support)
  - fseeko (seek to file position with large file support)
  - SEEK_SET (standard seek mode constant)
- Called from:
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (multiple call sites for archive initialization)

## Notes and Other Information
- Function is non-static and used by custom archive format implementation
- Uses large file support variants (ftello/fseeko) rather than standard ftell/fseek for handling files larger than 2GB
- Contains important implementation note about platform differences in SEEK_CUR behavior
- Critical for determining archive format capabilities during initialization
- Returns false for streams like pipes or sockets that don't support seeking
- Enables archive code to choose appropriate strategies based on stream capabilities
- Simple but essential function for robust file handling in archive operations