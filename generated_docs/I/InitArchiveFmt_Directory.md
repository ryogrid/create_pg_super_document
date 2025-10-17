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

## Simplified Source

```c
void InitArchiveFmt_Directory(ArchiveHandle *AH) {
    // Set up all function pointers for directory format operations
    AH->ArchiveEntryPtr = _ArchiveEntry;
    AH->StartDataPtr = _StartData;
    AH->WriteDataPtr = _WriteData;
    AH->EndDataPtr = _EndData;
    AH->WriteBytePtr = _WriteByte;
    AH->ReadBytePtr = _ReadByte;
    AH->WriteBufPtr = _WriteBuf;
    AH->ReadBufPtr = _ReadBuf;
    AH->ClosePtr = _CloseArchive;
    AH->ReopenPtr = _ReopenArchive;
    AH->PrintTocDataPtr = _PrintTocData;
    AH->ReadExtraTocPtr = _ReadExtraToc;
    AH->WriteExtraTocPtr = _WriteExtraToc;
    AH->PrintExtraTocPtr = _PrintExtraToc;
    // Set large object handlers
    AH->StartLOsPtr = _StartLOs;
    AH->StartLOPtr = _StartLO;
    AH->EndLOPtr = _EndLO;
    AH->EndLOsPtr = _EndLOs;
    // Set parallel processing handlers
    AH->PrepParallelRestorePtr = _PrepParallelRestore;
    AH->ClonePtr = _Clone;
    AH->DeClonePtr = _DeClone;
    AH->WorkerJobRestorePtr = _WorkerJobRestoreDirectory;
    AH->WorkerJobDumpPtr = _WorkerJobDumpDirectory;

    // Initialize local context
    lclContext *ctx = pg_malloc0(sizeof(lclContext));
    AH->formatData = ctx;
    ctx->dataFH = NULL;
    ctx->LOsTocFH = NULL;

    if (!AH->fSpec || strcmp(AH->fSpec, "") == 0) {
        pg_fatal("no output directory specified");
    }
    ctx->directory = AH->fSpec;

    if (AH->mode == archModeWrite) {
        // Write mode: create or validate directory
        struct stat st;
        bool is_empty = false;

        // Check if directory exists and is empty
        if (stat(ctx->directory, &st) == 0 && S_ISDIR(st.st_mode)) {
            DIR *dir = opendir(ctx->directory);
            if (dir) {
                struct dirent *d;
                is_empty = true;
                while ((d = readdir(dir))) {
                    if (strcmp(d->d_name, ".") != 0 && strcmp(d->d_name, "..") != 0) {
                        is_empty = false;
                        break;
                    }
                }
                closedir(dir);
            }
        }

        // Create directory if it doesn't exist or is not empty
        if (!is_empty && mkdir(ctx->directory, 0700) < 0) {
            pg_fatal("could not create directory \"%s\": %m", ctx->directory);
        }
    } else {
        // Read mode: open and read TOC file
        char fname[MAXPGPATH];
        setFilePath(AH, fname, "toc.dat");

        CompressFileHandle *tocFH = InitDiscoverCompressFileHandle(fname, PG_BINARY_R);
        if (!tocFH) {
            pg_fatal("could not open input file \"%s\": %m", fname);
        }

        ctx->dataFH = tocFH;

        // Read TOC using tar format compatibility
        AH->format = archTar;
        ReadHead(AH);
        AH->format = archDirectory;
        ReadToc(AH);

        EndCompressFileHandle(tocFH);
        ctx->dataFH = NULL;
    }
}
```