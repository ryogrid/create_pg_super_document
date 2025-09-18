# InitArchiveFmt_Custom

## Location
[src/bin/pg_dump/pg_backup_custom.c:106-198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L106-L198)

## Overview
InitArchiveFmt_Custom is the initialization routine for the custom archive format in PostgreSQL's pg_dump utility, responsible for setting up function pointers and preparing the archive for reading or writing operations.

## Definition


## Detailed Description
This function serves as the global initialization routine required by the custom archive format. It performs several critical tasks:

1. **Function Pointer Setup**: Assigns all necessary function pointers to the ArchiveHandle structure, including data I/O operations, TOC handling, and parallel processing functions.

2. **Context Initialization**: Creates and initializes a local context (lclContext) to store format-specific data.

3. **File Operations**: Opens the appropriate input or output file based on the archive mode (read/write) and handles both file-based and stdin/stdout operations.

4. **Archive Preparation**: For read mode, loads the header and table of contents (TOC) from the archive file and records the position of the first data block.

The function supports both sequential and parallel restore operations but only provides parallel restore capabilities (no parallel dump support for custom format).

## Parameters / Member Variables
- : Pointer to the ArchiveHandle structure that contains all archive-related information and function pointers

## Dependencies
- Functions called/Symbols referenced:
  - [_ArchiveEntry](../A/_ArchiveEntry.md), _StartData, _WriteData, _EndData
  - [_WriteByte](../W/_WriteByte.md), _ReadByte, _WriteBuf, _ReadBuf
  - [_CloseArchive](../C/_CloseArchive.md), _ReopenArchive, _PrintTocData
  - [_ReadExtraToc](../R/_ReadExtraToc.md), _WriteExtraToc, _PrintExtraToc
  - [_StartLOs](../S/_StartLOs.md), _StartLO, _EndLO, _EndLOs
  - [_PrepParallelRestore](../P/_PrepParallelRestore.md), _Clone, _DeClone
  - [_WorkerJobRestoreCustom](../W/_WorkerJobRestoreCustom.md)
  - pg_malloc0, fopen, checkSeek, ReadHead, ReadToc, _getFilePos

- Called from (representative examples):
  - [_allocAH](../a/_allocAH.md) (in pg_backup_archiver.c:2449)

## Notes and Other Information
- This function is format-specific and must be declared in pg_backup_archiver.h as it's used by the global archive allocation routine
- The custom format supports seeking operations when the underlying file handle allows it
- Error handling includes proper file opening checks with descriptive error messages using pg_fatal()
- The function distinguishes between write mode (archModeWrite) and read mode for different initialization paths
- In read mode, the function positions the file pointer after reading the TOC to prepare for data block access