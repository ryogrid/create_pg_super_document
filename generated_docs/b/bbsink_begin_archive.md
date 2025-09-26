# bbsink_begin_archive

## Location
[src/include/backup/basebackup_sink.h:191-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/backup/basebackup_sink.h#L191-L199)

## Overview
Initiates the beginning of a new archive within a backup sink by calling the sink-specific archive initialization operation.

## Definition

```c
static inline void
bbsink_begin_archive(bbsink *sink, const char *archive_name)
```
## Detailed Description
This inline function serves as a wrapper to begin a new archive within the PostgreSQL base backup system. It delegates to the sink-specific begin_archive operation, which handles the actual creation and initialization of an archive with the specified name. The function is part of PostgreSQL's backup sink architecture that supports different output formats and destinations for backup data.

## Parameters / Member Variables
- : Pointer to the backup sink structure that will handle the archive operations
- : Name of the archive to be created (null-terminated string)

## Dependencies
- Functions called/Symbols referenced:
  - bbsink (structure type)
  - Assert (assertion macro)
- Called from (representative examples):
  - perform_base_backup
  - bbsink_gzip_begin_archive
  - bbsink_lz4_begin_archive
  - bbsink_forward_begin_archive
  - bbsink_zstd_begin_archive

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Includes assertion to ensure sink is not NULL for defensive programming
- The function delegates actual archive creation to sink-specific implementation through function pointer
- Part of PostgreSQL's pluggable backup sink architecture allowing different compression and archiving formats
- Called multiple times during a backup session as different archives (like base backup and WAL files) are created