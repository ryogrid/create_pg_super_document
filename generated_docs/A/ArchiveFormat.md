# ArchiveFormat

## Location
[src/bin/pg_dump/pg_backup.h:46-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup.h#L46-L47)

## Overview
An enumeration that defines the different output formats supported by pg_dump for database backup archives.

## Definition
```c
typedef enum _archiveFormat
{
    archUnknown = 0,
    archCustom = 1,
    archTar = 3,
    archNull = 4,
    archDirectory = 5,
} ArchiveFormat;
```

## Detailed Description
The `ArchiveFormat` enum specifies the various output formats that pg_dump can generate when creating database backups. Each format has different characteristics in terms of compression, portability, and restoration capabilities. The enum values are not sequential, likely for historical compatibility reasons.

## Parameters / Member Variables
- `archUnknown` (0): Represents an unspecified or invalid archive format
- `archCustom` (1): PostgreSQL's custom binary format, supports compression and selective restoration
- `archTar` (3): TAR archive format, portable but with some limitations
- `archNull` (4): Null format, typically used for testing or when no actual output is desired
- `archDirectory` (5): Directory format where each table/object is stored as a separate file

## Dependencies
- Functions called/Symbols referenced:
  - None (primitive enum type)
- Called from (representative examples):
  - [CreateArchive](../C/CreateArchive.md) and `OpenArchive` functions in pg_backup_archiver.c
  - [main](../m/main.md) function in pg_dump.c
  - [parseArchiveFormat](../p/parseArchiveFormat.md) function for command-line argument parsing
  - `_archiveHandle` struct as a member variable

## Notes and Other Information
This enum is central to pg_dump's operation, determining how the backup data is structured and stored. The custom format (archCustom) is the most feature-rich, supporting selective restoration and compression. The directory format (archDirectory) is useful for parallel dumps and when individual table access is needed. The missing enum value 2 suggests a deprecated or removed format from earlier PostgreSQL versions.