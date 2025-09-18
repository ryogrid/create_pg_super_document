# _ReadByte

## Location
src/bin/pg_dump/pg_backup_custom.c: 686 - 703

## Overview
Reads a single byte of data from the PostgreSQL custom format archive file with mandatory EOF error handling.

## Definition
```c
static int _ReadByte(ArchiveHandle *AH)
```

## Detailed Description
This function is a fundamental building block for reading data from PostgreSQL custom format archives. It provides a simple interface for reading individual bytes from the archive file. The function is marked as "Mandatory" in the comments, indicating it is a required implementation for the custom archive format.

The function serves as a wrapper around the standard library getc() function, adding critical error handling for EOF conditions. Unlike standard getc() which returns EOF for both errors and end-of-file, this function treats any EOF as a fatal error condition, which is appropriate for archive processing where unexpected end-of-file indicates corruption or incomplete data.

## Parameters / Member Variables
- `AH`: Archive handle containing the file handle and archive context for reading operations

## Dependencies
- Functions called/Symbols referenced:
  - READ_ERROR_EXIT: Error handling macro for read failures and EOF conditions
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md): Used during custom format archive initialization
  - lclTocEntry: Used in directory format implementation
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md): Used in directory format initialization

## Notes and Other Information
- This is a static function specific to the custom format archive handling
- Returns the byte value (0-255) on success, exits on error or EOF via READ_ERROR_EXIT macro
- Part of the mandatory interface that archive formats must implement
- EOF is treated as a fatal error, not a normal end-of-stream condition
- Used extensively throughout the pg_dump/pg_restore infrastructure for low-level data reading
- Complementary function to _WriteByte for archive I/O operations
- File location: src/bin/pg_dump/pg_backup_custom.c:686-703