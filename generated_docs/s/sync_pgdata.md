# sync_pgdata

## Location
[src/common/file_utils.c:97-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/file_utils.c#L97-L219)

## Overview
Synchronizes a PostgreSQL data directory (PGDATA) and all its contents to ensure data durability by flushing all data to persistent storage using either syncfs() or fsync() methods.

## Definition

```c
struct stat st;
```
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
  - [opendir](../o/opendir.md)/readdir/closedir
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

## Simplified Source

```c
void sync_pgdata(const char *pg_data, int serverVersion, DataDirSyncMethod sync_method)
{
    bool xlog_is_symlink;
    char pg_wal[MAXPGPATH];
    char pg_tblspc[MAXPGPATH];

    // Build paths - handle pg_xlog vs pg_wal naming change
    snprintf(pg_wal, MAXPGPATH, "%s/%s", pg_data,
             serverVersion < MINIMUM_VERSION_FOR_PG_WAL ? "pg_xlog" : "pg_wal");
    snprintf(pg_tblspc, MAXPGPATH, "%s/pg_tblspc", pg_data);

    // Check if WAL directory is a symlink
    struct stat st;
    xlog_is_symlink = false;
    if (lstat(pg_wal, &st) >= 0 && S_ISLNK(st.st_mode))
        xlog_is_symlink = true;

    switch (sync_method)
    {
        case DATA_DIR_SYNC_METHOD_SYNCFS:
            // Use Linux syncfs() for whole filesystem sync
            do_syncfs(pg_data);

            // Sync each tablespace filesystem
            DIR *dir = opendir(pg_tblspc);
            if (dir != NULL) {
                struct dirent *de;
                while ((de = readdir(dir)) != NULL) {
                    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
                        continue;
                    char subpath[MAXPGPATH * 2];
                    snprintf(subpath, sizeof(subpath), "%s/%s", pg_tblspc, de->d_name);
                    do_syncfs(subpath);
                }
                closedir(dir);
            }

            // Sync WAL if it's a symlink
            if (xlog_is_symlink)
                do_syncfs(pg_wal);
            break;

        case DATA_DIR_SYNC_METHOD_FSYNC:
            // Use traditional fsync() on individual files
            // Optional pre-sync hint for performance
            #ifdef PG_FLUSH_DATA_WORKS
                walkdir(pg_data, pre_sync_fname, false);
                if (xlog_is_symlink)
                    walkdir(pg_wal, pre_sync_fname, false);
                walkdir(pg_tblspc, pre_sync_fname, true);
            #endif

            // Perform actual fsync operations
            walkdir(pg_data, fsync_fname, false);
            if (xlog_is_symlink)
                walkdir(pg_wal, fsync_fname, false);
            walkdir(pg_tblspc, fsync_fname, true);
            break;
    }
}
```