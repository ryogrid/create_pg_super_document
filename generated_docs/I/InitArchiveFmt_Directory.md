# InitArchiveFmt_Directory

## Location
[src/bin/pg_dump/pg_backup_directory.c:109-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_directory.c#L109-L229)

## Overview
This function initializes the directory format archive handler for PostgreSQL's pg_dump utility, setting up all necessary function pointers and context for reading or writing directory-format database dumps.

## Definition

```c
struct stat st;
```
## Detailed Description
 serves as the initialization routine for the directory archive format in pg_dump. This function is responsible for:

1. **Function Pointer Assignment**: Sets up all the required function pointers in the ArchiveHandle structure to handle directory-specific operations (archiving, data handling, I/O operations, etc.)

2. **Context Initialization**: Creates and initializes a local context () that maintains directory-specific state information

3. **Directory Management**: In write mode, validates and creates the target directory if necessary, ensuring it's either non-existent or empty. In read mode, opens and reads the TOC (Table of Contents) file.

4. **Format Compatibility**: Handles the TOC file using tar format compatibility while maintaining directory format semantics

The function supports both dump creation (write mode) and dump restoration (read mode), adapting its behavior accordingly.

## Parameters / Member Variables
- : Pointer to the ArchiveHandle structure that contains all archive-related state and function pointers

## Dependencies
- Functions called/Symbols referenced:
  - [_ArchiveEntry](../A/_ArchiveEntry.md), _StartData, _WriteData, _EndData
  - [_WriteByte](../W/_WriteByte.md), _ReadByte, _WriteBuf, _ReadBuf
  - [_CloseArchive](../C/_CloseArchive.md), _ReopenArchive, _PrintTocData
  - [_ReadExtraToc](../R/_ReadExtraToc.md), _WriteExtraToc, _PrintExtraToc
  - [_StartLOs](../S/_StartLOs.md), _StartLO, _EndLO, _EndLOs
  - [_PrepParallelRestore](../P/_PrepParallelRestore.md), _Clone, _DeClone
  - [_WorkerJobRestoreDirectory](../W/_WorkerJobRestoreDirectory.md), _WorkerJobDumpDirectory
  - [pg_malloc0](../p/pg_malloc0.md), setFilePath, InitDiscoverCompressFileHandle
  - [ReadHead](../R/ReadHead.md), ReadToc, EndCompressFileHandle
- Called from:
  - [_allocAH](../a/_allocAH.md) (in pg_backup_archiver.c:2457)

## Notes and Other Information
- The directory format stores each database object as a separate file within a directory structure
- The TOC file (toc.dat) uses tar format internally for compatibility while the overall archive uses directory format
- In write mode, the function ensures the target directory is either non-existent (will be created) or empty
- The function performs comprehensive error checking for directory operations and file I/O
- The local context maintains file handles for data and LOBs (Large Objects) TOC files