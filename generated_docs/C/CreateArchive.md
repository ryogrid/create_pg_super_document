# CreateArchive

## Location
[src/bin/pg_dump/pg_backup_archiver.c:221-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L221-L236)

## Overview
Creates a new archive handle for PostgreSQL dump operations with the specified format, compression, and configuration parameters.

## Definition
```c
Archive *CreateArchive(const char *FileSpec, const ArchiveFormat fmt,
                      const pg_compress_specification compression_spec,
                      bool dosync, ArchiveMode mode,
                      SetupWorkerPtrType setupDumpWorker,
                      DataDirSyncMethod sync_method)
```

## Detailed Description
CreateArchive is the primary entry point for creating new archive handles in the PostgreSQL backup system. This function serves as a public wrapper around the internal _allocAH function, providing a clean interface for initializing archives with various formats and configurations.

The function allocates and initializes an ArchiveHandle structure, configuring it for the specified archive format (custom, tar, directory, or null), compression settings, synchronization options, and operational mode. It supports both file-based and stdio-based operations and handles format-specific initialization through dedicated initialization functions.

This is a foundational function in the pg_dump architecture, creating the central data structure that manages all aspects of the dump process including metadata handling, data serialization, and worker coordination.

## Parameters / Member Variables
- `FileSpec`: Path to the archive file, or NULL for stdio operations
- `fmt`: Archive format (archCustom, archTar, archDirectory, archNull, archUnknown)
- `compression_spec`: Compression specification structure defining algorithm and parameters
- `dosync`: Boolean flag indicating whether to sync data to disk immediately
- `mode`: Archive mode (archModeWrite for creating, archModeRead for reading, archModeAppend for appending)
- `setupDumpWorker`: Function pointer for setting up dump worker processes in parallel operations
- `sync_method`: Method for synchronizing data directory operations

## Dependencies
- Functions called/Symbols referenced:
  - [_allocAH](../a/_allocAH.md): Internal function that performs the actual archive handle allocation and initialization
  - [ArchiveHandle](../A/ArchiveHandle.md): Internal archive structure containing all operational state
  - [ArchiveFormat](../A/ArchiveFormat.md): Enum defining supported archive formats
  - [ArchiveMode](../A/ArchiveMode.md): Enum defining archive operation modes
  - [pg_compress_specification](../p/pg_compress_specification.md): Structure specifying compression parameters
  - [DataDirSyncMethod](../D/DataDirSyncMethod.md): Enum for data directory synchronization methods
- Called from (representative examples):
  - [main](../m/main.md): Primary entry point in pg_dump.c creates archives for dump operations

## Notes and Other Information
- The function returns a pointer to the public Archive interface, hiding the internal ArchiveHandle implementation
- Format-specific initialization is handled by _allocAH, which calls appropriate InitArchiveFmt_* functions
- Supports stdio operations when FileSpec is NULL
- The setupDumpWorker parameter enables parallel dump operations by providing worker setup functionality
- [Archive](../A/Archive.md) format can be archUnknown, in which case the format is automatically discovered
- The function establishes the foundation for all subsequent dump operations including TOC management, data serialization, and worker coordination
- Memory allocated by this function should be properly cleaned up using appropriate cleanup functions

## Simplified Source

```c
Archive *
CreateArchive(const char *FileSpec, const ArchiveFormat fmt,
              const pg_compress_specification compression_spec,
              bool dosync, ArchiveMode mode,
              SetupWorkerPtrType setupDumpWorker,
              DataDirSyncMethod sync_method)
{
    // Allocate and initialize new archive handle
    ArchiveHandle *AH = _allocAH(FileSpec, fmt, compression_spec,
                                 dosync, mode, setupDumpWorker, sync_method);

    // Return as public interface
    return (Archive *) AH;
}
```