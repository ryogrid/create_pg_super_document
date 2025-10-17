# _WriteByte

## Location
[src/bin/pg_dump/pg_backup_custom.c:669-685](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L669-L685)

## Overview
Writes a single byte of data to the PostgreSQL custom format archive file.

## Definition
```c
static int _WriteByte(ArchiveHandle *AH, const int i)
```

## Detailed Description
This function is a fundamental building block for writing data to PostgreSQL custom format archives. It provides a simple interface for writing individual bytes to the archive file. The function is marked as "Mandatory" in the comments, indicating it is a required implementation for the custom archive format.

The function serves as a wrapper around the standard library fputc() function, adding error handling specific to the archive writing context. It writes the least significant byte of the provided integer value to the archive file.

## Parameters / Member Variables
- `AH`: Archive handle containing the file handle and archive context
- `i`: Integer value whose least significant byte will be written to the archive

## Dependencies
- Functions called/Symbols referenced:
  - WRITE_ERROR_EXIT: Error handling macro for write failures
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md): Used during custom format archive initialization
  - [_StartData](../S/_StartData.md): Used when beginning data section writing
  - [_StartLOs](../S/_StartLOs.md): Used when beginning Large Object section writing
  - lclTocEntry: Used in directory format implementation
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md): Used in directory format initialization
  - [InitArchiveFmt_Null](../I/InitArchiveFmt_Null.md): Used in null format initialization

## Notes and Other Information
- This is a static function specific to the custom format archive handling
- Returns 1 on success, exits on error via WRITE_ERROR_EXIT macro
- Part of the mandatory interface that archive formats must implement
- Used extensively throughout the pg_dump/pg_restore infrastructure
- The function writes only the least significant byte of the input integer
- File location: src/bin/pg_dump/pg_backup_custom.c:669-685

## Simplified Source
```c
static int _WriteByte(ArchiveHandle *AH, const int i)
{
    // Write single byte to archive file
    if (fputc(i, AH->FH) == EOF)
        WRITE_ERROR_EXIT;

    return 1;
}
```