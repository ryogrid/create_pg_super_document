# tablespaceinfo

## Location
src/include/backup/basebackup.h: 35 - 42

## Overview
The `tablespaceinfo` structure represents metadata about PostgreSQL tablespaces during backup operations, storing essential information needed for base backup processes and tablespace mapping.

## Definition

```c
struct IncrementalBackupInfo;
```
## Detailed Description
The `tablespaceinfo` structure is a central component of PostgreSQL's base backup infrastructure, defined in `src/include/backup/basebackup.h`. It encapsulates all necessary metadata about tablespaces that are included in backup operations, serving as the primary data structure for tracking tablespace information during backup processes.

This structure is used throughout the backup system to:
- Track individual tablespaces and their locations during base backup operations
- Manage the mapping between tablespace OIDs and their physical paths
- Calculate backup sizes for progress reporting
- Generate tablespace mapping files for backup restoration
- Handle both absolute and relative paths for tablespaces located within or outside PGDATA

In base backup operations, a list of `tablespaceinfo` structures is constructed to represent all tablespaces in the cluster, including the main data directory (represented with a NULL path). The structure supports both traditional symlink-based tablespaces and in-place tablespaces used for testing purposes.

## Parameters / Member Variables
- `oid`: The unique Object Identifier (OID) of the tablespace as stored in the system catalogs. This serves as the primary key for identifying the tablespace
- `path`: The full absolute path to the tablespace directory. For the main data directory (PGDATA), this field is set to NULL to distinguish it from user-defined tablespaces
- `rpath`: The relative path to the tablespace directory when it's located within PGDATA, otherwise NULL. This optimization allows for more portable backups when tablespaces are contained within the main data directory
- `size`: The total size of the tablespace in bytes as calculated during backup operations. Initially set to -1 to indicate unknown size, then populated during backup size estimation

## Dependencies
- Functions called/Symbols referenced:
  - SendBaseBackup
  - BaseBackupCmd
  - IncrementalBackupInfo
  
- Called from (representative examples):
  - do_pg_backup_start (src/backend/access/transam/xlog.c:8869, 9090)
  - perform_base_backup (src/backend/backup/basebackup.c:283, 290, 304, 323)
  - sendDir (src/backend/backup/basebackup.c:1447)
  - SendTablespaceList (src/backend/backup/basebackup_copy.c:398)
  - bbsink_copystream_begin_archive (src/backend/backup/basebackup_copy.c:168)
  - read_tablespace_map (src/backend/access/transam/xlogrecovery.c:1356, 1409)
  - InitWalRecovery (src/backend/access/transam/xlogrecovery.c:676)

## Notes and Other Information
- The structure is primarily used in list form, where multiple `tablespaceinfo` entries represent all tablespaces in a cluster during backup operations
- The main data directory is always represented as the last entry in tablespace lists, with a NULL path value
- Size calculation (`size` field) is performed on-demand during backup operations when progress reporting is enabled
- The `rpath` field enables more portable backups by storing relative paths for tablespaces contained within PGDATA
- This structure is essential for generating the `tablespace_map` file, which is required for proper backup restoration, especially in Windows environments where symbolic links may not be available
- In incremental backup scenarios, the structure works in conjunction with `IncrementalBackupInfo` to track changes in tablespaces
- The structure supports both symbolic link-based tablespaces (traditional) and directory-based tablespaces (testing with allow_in_place_tablespaces)