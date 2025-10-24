# OpenArchive

## Location
[src/bin/pg_dump/pg_backup_archiver.c:237-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L237-L251)

## Overview
Opens an existing PostgreSQL dump archive file for restoration operations, initializing the necessary data structures and handlers for reading archive content.

## Definition

```c
Archive *
OpenArchive(const char *FileSpec, const ArchiveFormat fmt)
```
## Detailed Description
The OpenArchive function is responsible for opening and initializing an existing PostgreSQL dump archive file for restoration purposes. It creates an ArchiveHandle structure that manages the archive operations throughout the restoration process. The function sets up compression specifications (defaulting to no compression) and allocates the archive handle with appropriate parameters for read operations.

The function operates in read mode (archModeRead) and configures the archive to use the setupRestoreWorker function for worker process management. It also sets the data directory synchronization method to fsync for data integrity.

## Parameters / Member Variables
- `*FileSpec`: Path to the archive file to be opened
- `fmt`: Format of the archive (ArchiveFormat enum value)
## Dependencies
- Functions called/Symbols referenced:
  - [_allocAH](../a/_allocAH.md)
  - [ArchiveFormat](../A/ArchiveFormat.md)
  - [pg_compress_specification](../p/pg_compress_specification.md)
  - PG_COMPRESSION_NONE
  - archModeRead
  - [setupRestoreWorker](../s/setupRestoreWorker.md)
  - DATA_DIR_SYNC_METHOD_FSYNC
- Called from (representative examples):
  - [main](../m/main.md) (in pg_restore.c)

## Notes and Other Information
- This function is used specifically for restoration operations, as indicated by the archModeRead parameter
- The function initializes compression specification with no compression by default
- The returned Archive pointer is actually an ArchiveHandle cast to Archive type
- This is a public function in the pg_dump/pg_restore architecture
- The function is typically called during the initialization phase of pg_restore operations

## Simplified Source

```c
Archive *
OpenArchive(const char *FileSpec, const ArchiveFormat fmt)
{
    ArchiveHandle *AH;
    pg_compress_specification compression_spec = {0};

    // Initialize compression spec (no compression by default)
    compression_spec.algorithm = PG_COMPRESSION_NONE;

    // Allocate archive handle for reading
    AH = _allocAH(FileSpec, fmt, compression_spec, true,
                  archModeRead, setupRestoreWorker,
                  DATA_DIR_SYNC_METHOD_FSYNC);

    return (Archive *) AH;
}
```