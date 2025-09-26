# StatusFilePath

## Location
[src/include/access/xlog_internal.h:238-243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L238-L243)

## Overview
StatusFilePath is an inline function that constructs the complete file system path for WAL archive status files, which track the archival state of WAL segments and timeline history files.

## Definition

```c
static inline void
StatusFilePath(char *path, const char *xlog, const char *suffix)
```
## Detailed Description
This function generates the full file system path for archive status files by combining the WAL directory path, the archive_status subdirectory, the base filename, and a status suffix. Archive status files are used by PostgreSQL's WAL archiving mechanism to track whether WAL files are ready for archiving, currently being archived, or have been successfully archived. The resulting path follows the pattern "pg_wal/archive_status/FILENAME.SUFFIX" where:
- FILENAME is the WAL segment or timeline history file name
- SUFFIX indicates the archival status (.ready, .done, .backup, etc.)

This function is central to PostgreSQL's WAL archiving infrastructure, enabling reliable tracking of file archival states.

## Parameters / Member Variables
- : Output buffer that receives the constructed file path (must be at least MAXPGPATH bytes)
- : Base filename of the WAL file or timeline history file being tracked
- : Status suffix indicating the archival state (e.g., ".ready", ".done")

## Dependencies
- Functions called/Symbols referenced:
  - XLOGDIR (macro defining the WAL directory path, typically "pg_wal")
- Called from (representative examples):
  - [XLogArchiveNotify](../X/XLogArchiveNotify.md) (creates .ready status files for archiving)
  - [XLogArchiveForceDone](../X/XLogArchiveForceDone.md) (marks files as .done)
  - [XLogArchiveCheckDone](../X/XLogArchiveCheckDone.md) (checks for .done status files)
  - [XLogArchiveIsBusy](../X/XLogArchiveIsBusy.md) (checks for archiving in progress)
  - [pgarch_readyXlog](../p/pgarch_readyXlog.md) (archiver process reads status files)
  - [pgarch_archiveDone](../p/pgarch_archiveDone.md) (marks archival completion)

## Notes and Other Information
- This is an inline function defined in the header for performance optimization
- The function uses snprintf for safe string formatting with buffer bounds checking
- [Archive](../A/Archive.md) status files are small marker files that contain no actual data - their existence indicates state
- Common suffixes include .ready (ready for archiving), .done (successfully archived), .backup (backup in progress)
- The archive_status subdirectory isolates status files from actual WAL data files
- Status files are critical for ensuring reliable WAL archiving and preventing data loss
- The archiver process continuously monitors the archive_status directory for new .ready files
- Status files are automatically cleaned up after successful archival operations