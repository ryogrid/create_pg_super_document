# _WriteExtraToc

## Location
[src/bin/pg_dump/pg_backup_custom.c:222-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L222-L237)

## Overview
_WriteExtraToc is a callback function that saves format-specific TOC entry data to the archive, specifically writing data position offset information for the custom archive format.

## Definition

```c
static void
_WriteExtraToc(ArchiveHandle *AH, TocEntry *te)
```
## Detailed Description
This function serves as an optional callback invoked by the PostgreSQL archiver to save extra format-related data associated with each Table of Contents (TOC) entry. For the custom archive format, this involves writing the data position and state information that was previously set up during archive creation.

The function retrieves the local TOC entry context (lclTocEntry) that was attached to the TOC entry during the _ArchiveEntry call, and then uses the archiver's WriteOffset routine to save the data position (dataPos) and its state (dataState) to the archive file.

This mechanism allows the custom format to maintain precise offset information for efficient seeking during archive restoration, enabling random access to data blocks within the archive file.

## Parameters / Member Variables
- : Pointer to the ArchiveHandle structure containing archive context and I/O functions
- : Pointer to the TocEntry structure whose extra format data needs to be written

## Dependencies
- Functions called/Symbols referenced:
  - [WriteOffset](WriteOffset.md) (archiver utility function for writing offset data)
  - lclTocEntry (local TOC entry structure type)

- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (assigned as WriteExtraTocPtr function pointer)
  - Referenced by InitArchiveFmt_Directory (directory format also uses this pattern)

## Notes and Other Information
- This function is declared as static, limiting its scope to the pg_backup_custom.c file
- The function is optional in the archive format interface but is crucial for the custom format's seeking capabilities
- Uses archiver-provided routines (WriteOffset) to ensure proper endianness handling and file format consistency
- The data written here will be read back by the corresponding _ReadExtraToc function during archive restoration
- The offset information enables efficient random access to data blocks, which is a key feature of the custom archive format
- This function is part of the format-specific function pointer interface that makes the archive system extensible