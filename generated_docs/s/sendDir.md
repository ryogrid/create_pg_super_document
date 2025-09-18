# sendDir

## Location
src/backend/backup/basebackup.c: 1187 - 1571

## Overview
sendDir recursively includes all files from a given directory in the output tar stream during PostgreSQL base backup operations, with comprehensive filtering and special handling for various file types.

## Definition
```c
static int64 sendDir(bbsink *sink, const char *path, int basepathlen, bool sizeonly,
                    List *tablespaces, bool sendtblspclinks, backup_manifest_info *manifest,
                    Oid spcoid, IncrementalBackupInfo *ib)
```

## Detailed Description
This function is the core directory traversal component of PostgreSQL's base backup system. It recursively processes directory contents, applying sophisticated filtering logic to determine which files to include or exclude. The function handles relation files, temporary files, unlogged tables, tablespace symlinks, and various PostgreSQL-specific directories with special processing rules. It supports both full and incremental backup modes, can operate in size-only calculation mode, and maintains backup manifest information throughout the process.

Key processing logic includes:
- Detection of database directories containing relations
- Exclusion of temporary files, unlogged relations (except init forks), and system files
- Special handling of pg_wal, pg_tblspc, and other PostgreSQL directories
- Support for incremental backups with block-level granularity
- Recursive directory traversal with tablespace awareness

## Parameters / Member Variables
- `sink`: bbsink object representing the backup destination stream
- `path`: File system path to the directory being processed
- `basepathlen`: Length of the base path for tar header name calculation
- `sizeonly`: Boolean flag - if true, only calculates total size without sending data
- `tablespaces`: List of tablespace information to avoid duplicate backups
- `sendtblspclinks`: Boolean flag indicating whether to include tablespace symlink information
- `manifest`: Pointer to backup manifest information structure for tracking backup contents
- `spcoid`: Object identifier (OID) of the current tablespace
- `ib`: Pointer to incremental backup information structure (NULL for full backups)

## Dependencies
- Functions called/Symbols referenced:
  - AllocateDir, ReadDir, FreeDir
  - lstat, readlink
  - parse_filename_for_nontemp_relation
  - looks_like_temp_rel_name
  - [sendFile](sendFile.md)
  - [_tarWriteHeader](../t/_tarWriteHeader.md)
  - [GetFileBackupMethod](../G/GetFileBackupMethod.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [convert_link_to_directory](../c/convert_link_to_directory.md)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md)
  - [sendTablespace](sendTablespace.md)
  - [sendDir](sendDir.md) (recursive calls)

## Notes and Other Information
- Recursively calls itself for subdirectories, making it the primary directory traversal mechanism
- Implements complex filtering logic to exclude temporary files, unlogged relations, and system-specific files
- Handles incremental backups by determining which file blocks need to be backed up
- Special processing for pg_wal directory (included as empty) and pg_tblspc (symlink handling)
- Supports interruption checking and recovery state validation during long-running operations
- Uses a large BlockNumber array (RELSEG_SIZE) allocated on heap for incremental backup block tracking
- Located in src/backend/backup/basebackup.c:1187-1571