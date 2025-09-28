# SyncDataDirectory

## Location
[src/backend/storage/file/fd.c:3541-3654](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3541-L3654)

## Overview
Issues fsync recursively on PGDATA and all its contents, or alternatively uses syncfs for filesystem-wide synchronization, ensuring that all pending writes reach disk during database startup recovery.

## Definition

```c
struct stat st;
```
## Detailed Description
SyncDataDirectory is a critical function used during PostgreSQL startup to synchronize the entire data directory to persistent storage. The function handles the possibility that there are issued-but-unsynced writes pending against the data directory from a previous session.

The function operates in different modes based on the  setting:
1. **syncfs mode** (Linux only): Uses syncfs() to sync entire filesystems, which is more efficient
2. **fsync mode**: Recursively walks through all directories and files, issuing individual fsync() calls

The function carefully handles symlinks - it follows symlinks only for pg_wal and directories immediately under pg_tblspc, but ignores other symlinks as they are presumed to point at files PostgreSQL is not responsible for syncing.

Error handling is designed to be non-fatal - errors are logged but don't abort startup, preventing failures due to harmless cases like read-only files in the data directory.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - lstat: Check if pg_wal is a symlink
  - S_ISLNK: Test symlink status
  - [begin_startup_progress_phase](../b/begin_startup_progress_phase.md): Progress reporting during startup
  - [do_syncfs](../d/do_syncfs.md): Perform syncfs() operation (Linux only)
  - [AllocateDir](../A/AllocateDir.md)/ReadDirExtended/FreeDir: Directory traversal
  - [walkdir](../w/walkdir.md): Recursive directory walking
  - [pre_sync_fname](../p/pre_sync_fname.md): Pre-sync hint callback
  - [datadir_fsync_fname](../d/datadir_fsync_fname.md): fsync callback function
- Called from (representative examples):
  - [StartupXLOG](StartupXLOG.md): During WAL recovery startup

## Notes and Other Information
- The function assumes it's already chdir'd into PGDATA
- Can be completely skipped if fsync is disabled via enableFsync
- Uses conditional compilation for Linux-specific syncfs functionality
- Implements a two-phase approach when using fsync: pre-sync hints followed by actual fsync operations
- Special handling for pg_wal symlinks and tablespace directories under pg_tblspc
- Progress reporting is integrated for startup monitoring
- Error tolerance design ensures startup robustness even with permission or access issues

## Simplified Source

```c
// Simplified version of SyncDataDirectory
void SyncDataDirectory(void) {
    bool xlog_is_symlink;

    // Skip if fsync is disabled
    if (!enableFsync)
        return;

    // Check if pg_wal is a symlink
    xlog_is_symlink = false;
    struct stat st;
    if (lstat("pg_wal", &st) >= 0 && S_ISLNK(st.st_mode))
        xlog_is_symlink = true;

#ifdef HAVE_SYNCFS
    // Linux: Use efficient syncfs() for entire filesystems
    if (recovery_init_sync_method == DATA_DIR_SYNC_METHOD_SYNCFS) {
        begin_startup_progress_phase();

        // Sync main data directory
        do_syncfs(".");

        // Sync each tablespace directory
        DIR *dir = AllocateDir("pg_tblspc");
        struct dirent *de;
        while ((de = ReadDirExtended(dir, "pg_tblspc", LOG))) {
            if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
                continue;
            char path[MAXPGPATH];
            snprintf(path, MAXPGPATH, "pg_tblspc/%s", de->d_name);
            do_syncfs(path);
        }
        FreeDir(dir);

        // Sync pg_wal if it's a symlink
        if (xlog_is_symlink)
            do_syncfs("pg_wal");
        return;
    }
#endif

    // Traditional approach: Individual fsync() calls
    begin_startup_progress_phase();

#ifdef PG_FLUSH_DATA_WORKS
    // Phase 1: Hint to kernel about upcoming fsyncs
    walkdir(".", pre_sync_fname, false, DEBUG1);
    if (xlog_is_symlink)
        walkdir("pg_wal", pre_sync_fname, false, DEBUG1);
    walkdir("pg_tblspc", pre_sync_fname, true, DEBUG1);
#endif

    begin_startup_progress_phase();

    // Phase 2: Actual fsync operations
    walkdir(".", datadir_fsync_fname, false, LOG);
    if (xlog_is_symlink)
        walkdir("pg_wal", datadir_fsync_fname, false, LOG);
    walkdir("pg_tblspc", datadir_fsync_fname, true, LOG);
}
```

Key simplifications made:
- Removed detailed error handling and logging for clarity
- Consolidated stat checking logic into simpler flow
- Abstracted complex directory traversal into high-level walkdir calls
- Simplified conditional compilation blocks
- Combined variable declarations with assignments where possible
- Focused on the two main sync strategies: syncfs vs individual fsync
- Preserved essential algorithm: check symlinks, choose sync method, execute sync operations