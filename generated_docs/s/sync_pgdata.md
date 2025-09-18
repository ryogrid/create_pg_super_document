# sync_pgdata

## Location
[src/common/file_utils.c:97-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/file_utils.c#L97-L219)

## Overview
Synchronizes a PostgreSQL data directory (PGDATA) and all its contents to ensure data durability by flushing all data to persistent storage using either syncfs() or fsync() methods.

## Definition


## Detailed Description
The  function is responsible for synchronizing the entire PostgreSQL data directory to ensure that all data is safely written to disk. It handles the complexity of different PostgreSQL versions and synchronization methods while carefully managing symbolic links.

The function supports two synchronization methods:
1. **SYNCFS method**: Uses the Linux-specific syncfs() system call to sync entire filesystems, which is more efficient for large data directories
2. **FSYNC method**: Uses traditional fsync() calls on individual files, with optional pre-sync hinting for better performance

Special handling is provided for:
- Version-specific directory names (pg_xlog vs pg_wal)
- Symbolic links in pg_wal and pg_tblspc directories
- Tablespace directories that may reside on different filesystems

## Parameters / Member Variables
- : Path to the PostgreSQL data directory (PGDATA) to synchronize
- : PostgreSQL server version number, used to determine directory naming conventions (affects pg_xlog vs pg_wal)
- : Synchronization method to use (DATA_DIR_SYNC_METHOD_SYNCFS or DATA_DIR_SYNC_METHOD_FSYNC)

## Dependencies
- Functions called/Symbols referenced:
  - [do_syncfs](../d/do_syncfs.md)
  - [walkdir](../w/walkdir.md)
  - [pre_sync_fname](../p/pre_sync_fname.md)
  - [fsync_fname](../f/fsync_fname.md)
  - opendir/readdir/closedir
  - lstat
  - S_ISLNK
- Called from (representative examples):
  - [main](../m/main.md) (initdb)
  - [BaseBackup](../B/BaseBackup.md) (pg_basebackup)
  - [main](../m/main.md) (pg_checksums)
  - [sync_target_dir](sync_target_dir.md) (pg_rewind)

## Notes and Other Information
- Handles version compatibility by checking MINIMUM_VERSION_FOR_PG_WAL to use correct WAL directory name
- Carefully manages symbolic links - follows symlinks only for pg_wal and tablespaces under pg_tblspc
- The syncfs method is Linux-specific and requires HAVE_SYNCFS compile-time support
- When using fsync method, performs pre-sync operations when PG_FLUSH_DATA_WORKS is available for performance optimization
- Tablespace directories may be processed twice in fsync mode to handle both regular directories and symlinks
- Critical for data durability in PostgreSQL utilities like initdb, pg_basebackup, and pg_rewind