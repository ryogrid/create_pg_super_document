# _ReadBuf

## Location
[src/bin/pg_dump/pg_backup_custom.c:718-739](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L718-L739)

## Overview
A low-level function that reads a block of bytes from the archive file in PostgreSQL's custom dump format.

## Definition

```c
static void
_ReadBuf(ArchiveHandle *AH, void *buf, size_t len)
```
## Detailed Description
_ReadBuf is a mandatory function in the custom archive format implementation that provides the basic file reading capability for pg_dump. It serves as a wrapper around the standard fread() function, reading exactly the specified number of bytes from the archive file. The function includes error handling to ensure that the read operation completes successfully or terminates the program with an appropriate error message.

This function is part of the custom archive format's function table and is called by the archiver infrastructure whenever raw data needs to be read from the archive file during restore operations.

## Parameters / Member Variables
- `*AH`: ArchiveHandle pointer containing the archive context and file handle (AH->FH)
- `*buf`: Destination buffer where the read data will be stored
- `len`: Number of bytes to read from the archive
## Dependencies
- Functions called/Symbols referenced:
  - fread (standard C library function)
  - READ_ERROR_EXIT (PostgreSQL macro for handling read errors)
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (custom format initialization)
  - [_CustomReadFunc](../C/_CustomReadFunc.md) (custom format read function)
  - lclTocEntry (directory format TOC entry handling)
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md) (directory format initialization)

## Notes and Other Information
- This is a static function specific to the custom archive format implementation
- The function provides no partial read handling - it either reads all requested bytes or exits with an error
- Located in src/bin/pg_dump/pg_backup_custom.c at lines 718-739
- Part of the mandatory function interface that archive formats must implement
- Uses the file handle stored in the ArchiveHandle structure (AH->FH)

## Simplified Source
```c
static void _ReadBuf(ArchiveHandle *AH, void *buf, size_t len)
{
    // Read entire buffer from archive file
    if (fread(buf, 1, len, AH->FH) != len)
        READ_ERROR_EXIT(AH->FH);
}
```