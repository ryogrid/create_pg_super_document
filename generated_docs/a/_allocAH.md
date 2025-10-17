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
  - [pg_malloc0](../p/pg_malloc0.md)
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

## Simplified Source

```c
static ArchiveHandle *
_allocAH(const char *FileSpec, const ArchiveFormat fmt,
         const pg_compress_specification compression_spec,
         bool dosync, ArchiveMode mode,
         SetupWorkerPtrType setupWorkerPtr, DataDirSyncMethod sync_method)
{
    ArchiveHandle *AH;
    CompressFileHandle *CFH;
    pg_compress_specification out_compress_spec = {0};

    pg_log_debug("allocating AH for %s, format %d",
                 FileSpec ? FileSpec : "(stdio)", fmt);

    // Allocate and initialize archive handle
    AH = (ArchiveHandle *) pg_malloc0(sizeof(ArchiveHandle));

    AH->version = K_VERS_SELF;

    // Set defaults for string processing and error handling
    AH->public.encoding = 0;        // PG_SQL_ASCII
    AH->public.std_strings = false;
    AH->public.exit_on_error = true;
    AH->public.n_errors = 0;

    AH->archiveDumpVersion = PG_VERSION;
    AH->createDate = time(NULL);
    AH->intSize = sizeof(int);
    AH->offSize = sizeof(pgoff_t);

    // Set file specification
    if (FileSpec)
        AH->fSpec = pg_strdup(FileSpec);
    else
        AH->fSpec = NULL;

    // Initialize context tracking
    AH->currUser = NULL;
    AH->currSchema = NULL;
    AH->currTablespace = NULL;
    AH->currTableAm = NULL;

    // Create circular TOC list
    AH->toc = (TocEntry *) pg_malloc0(sizeof(TocEntry));
    AH->toc->next = AH->toc;
    AH->toc->prev = AH->toc;

    // Set operational parameters
    AH->mode = mode;
    AH->compression_spec = compression_spec;
    AH->dosync = dosync;
    AH->sync_method = sync_method;

    memset(&(AH->sqlparse), 0, sizeof(AH->sqlparse));

    // Set up stdout compression handle
    out_compress_spec.algorithm = PG_COMPRESSION_NONE;
    CFH = InitCompressFileHandle(out_compress_spec);
    if (!CFH->open_func(NULL, fileno(stdout), PG_BINARY_A, CFH))
        pg_fatal("could not open stdout for appending: %m");
    AH->OF = CFH;

    // Platform-specific binary mode setup
#ifdef WIN32
    if ((fmt != archNull || compression_spec.algorithm != PG_COMPRESSION_NONE) &&
        (AH->fSpec == NULL || strcmp(AH->fSpec, "") == 0))
    {
        if (mode == archModeWrite)
            _setmode(fileno(stdout), O_BINARY);
        else
            _setmode(fileno(stdin), O_BINARY);
    }
#endif

    AH->SetupWorkerPtr = setupWorkerPtr;

    // Determine and set archive format
    if (fmt == archUnknown)
        AH->format = _discoverArchiveFormat(AH);
    else
        AH->format = fmt;

    // Initialize format-specific handlers
    switch (AH->format)
    {
        case archCustom:
            InitArchiveFmt_Custom(AH);
            break;
        case archNull:
            InitArchiveFmt_Null(AH);
            break;
        case archDirectory:
            InitArchiveFmt_Directory(AH);
            break;
        case archTar:
            InitArchiveFmt_Tar(AH);
            break;
        default:
            pg_fatal("unrecognized file format \"%d\"", fmt);
    }

    return AH;
}
```