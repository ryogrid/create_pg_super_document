# ArchiveMode

## Location
[src/bin/pg_dump/pg_backup.h:53-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup.h#L53-L54)

## Overview
An enumeration that defines the access modes for PostgreSQL archive operations, specifying whether an archive is being created, read from, or appended to.

## Definition
```c
typedef enum _archiveMode
{
    archModeAppend,
    archModeWrite,
    archModeRead,
} ArchiveMode;
```

## Detailed Description
The `ArchiveMode` enum specifies the operational mode for archive handling in pg_dump and pg_restore. It determines how the archive file or stream is accessed and what operations are permitted. This mode affects the internal behavior of archive handlers and ensures proper file access patterns.

## Parameters / Member Variables
- `archModeAppend`: Mode for appending data to an existing archive
- `archModeWrite`: Mode for writing/creating a new archive from scratch
- `archModeRead`: Mode for reading data from an existing archive during restoration

## Dependencies
- Functions called/Symbols referenced:
  - None (primitive enum type)
- Called from (representative examples):
  - [CreateArchive](../C/CreateArchive.md) function in pg_backup_archiver.c for setting up new archives
  - `_archiveHandle` struct as a member variable to track current mode
  - [main](../m/main.md) function in pg_dump.c for determining operation mode
  - [Archive](Archive.md) allocation and handling functions

## Notes and Other Information
This enum is essential for the proper operation of PostgreSQL's backup and restore utilities. The mode determines the file access patterns and internal state management of the archive handlers. Write mode is used during pg_dump operations, read mode during pg_restore, and append mode for incremental or continued operations. The mode affects buffer management, file positioning, and error handling within the archive system.