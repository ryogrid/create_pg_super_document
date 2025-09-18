# _allocAH

## Location
[src/bin/pg_dump/pg_backup_archiver.c:2355-2474](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L2355-L2474)

## Overview
_allocAH is a static function that allocates and initializes a new ArchiveHandle structure for PostgreSQL archive operations, setting up format-specific handlers and compression settings.

## Definition
```c
static ArchiveHandle *_allocAH(const char *FileSpec, const ArchiveFormat fmt,
                              const pg_compress_specification compression_spec,
                              bool dosync, ArchiveMode mode,
                              SetupWorkerPtrType setupWorkerPtr, DataDirSyncMethod sync_method)
```

## Detailed Description
This function serves as the primary constructor for ArchiveHandle objects in PostgreSQL's archive system. It initializes all essential fields, sets up compression handles, configures binary mode for Windows systems, determines the archive format (using _discoverArchiveFormat if unknown), and calls the appropriate format-specific initialization function. The function creates a circular linked list for the table of contents and establishes default values for encoding, error handling, and various operational parameters.

## Parameters / Member Variables
- `FileSpec`: const char pointer - path to the archive file, or NULL for stdin/stdout
- `fmt`: ArchiveFormat - the archive format type (custom, tar, directory, null, or unknown)
- `compression_spec`: pg_compress_specification - compression settings and algorithm
- `dosync`: bool - whether to perform filesystem synchronization
- `mode`: ArchiveMode - read or write mode for the archive
- `setupWorkerPtr`: SetupWorkerPtrType - function pointer for worker setup in parallel operations
- `sync_method`: DataDirSyncMethod - method for data directory synchronization

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_debug
  - pg_malloc0
  - [pg_strdup](../p/pg_strdup.md)
  - time
  - [InitCompressFileHandle](../I/InitCompressFileHandle.md)
  - [_discoverArchiveFormat](../d/_discoverArchiveFormat.md)
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md)
  - [InitArchiveFmt_Null](../I/InitArchiveFmt_Null.md)
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md)
  - InitArchiveFmt_Tar
- Called from (representative examples):
  - [CreateArchive](../C/CreateArchive.md)
  - [OpenArchive](../O/OpenArchive.md)

## Notes and Other Information
- Static function, only accessible within pg_backup_archiver.c
- Central allocation point for all archive handles in pg_dump/pg_restore
- Handles platform-specific binary mode setup for Windows
- Creates a self-referencing circular TOC entry as the list head
- Sets up compression for stdout output regardless of archive format
- Critical initialization function that determines archive behavior through format-specific handlers
- Version information is embedded using K_VERS_SELF constant