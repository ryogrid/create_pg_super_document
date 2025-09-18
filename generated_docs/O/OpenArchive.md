# OpenArchive

## Location
src/bin/pg_dump/pg_backup_archiver.c: 237 - 251

## Overview
Opens an existing PostgreSQL dump archive file for restoration operations, initializing the necessary data structures and handlers for reading archive content.

## Definition


## Detailed Description
The OpenArchive function is responsible for opening and initializing an existing PostgreSQL dump archive file for restoration purposes. It creates an ArchiveHandle structure that manages the archive operations throughout the restoration process. The function sets up compression specifications (defaulting to no compression) and allocates the archive handle with appropriate parameters for read operations.

The function operates in read mode (archModeRead) and configures the archive to use the setupRestoreWorker function for worker process management. It also sets the data directory synchronization method to fsync for data integrity.

## Parameters / Member Variables
- : Path to the archive file to be opened
- : Format of the archive (ArchiveFormat enum value)

## Dependencies
- Functions called/Symbols referenced:
  - _allocAH
  - ArchiveFormat
  - pg_compress_specification
  - PG_COMPRESSION_NONE
  - archModeRead
  - setupRestoreWorker
  - DATA_DIR_SYNC_METHOD_FSYNC
- Called from (representative examples):
  - main (in pg_restore.c)

## Notes and Other Information
- This function is used specifically for restoration operations, as indicated by the archModeRead parameter
- The function initializes compression specification with no compression by default
- The returned Archive pointer is actually an ArchiveHandle cast to Archive type
- This is a public function in the pg_dump/pg_restore architecture
- The function is typically called during the initialization phase of pg_restore operations