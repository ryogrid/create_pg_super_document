# KeepFileRestoredFromArchive

## Location
[src/backend/access/transam/xlogarchive.c:358-443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogarchive.c#L358-L443)

## Overview
Moves a file restored from archive storage from its temporary location to the permanent location in pg_wal, handling file replacement and necessary cleanup operations.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
KeepFileRestoredFromArchive finalizes the restoration of an archived file by moving it from its temporary restoration path to the permanent location in the pg_wal directory. The function handles the complexities of file replacement, particularly on Windows systems where open file handles can prevent immediate replacement.

The function implements platform-specific logic for safe file replacement - on Windows, it uses a unique temporary naming scheme to work around file locking issues with processes that may have the old file open. It also manages archive notification to prevent the restored file from being archived again and coordinates with WAL senders to ensure they reload the new file content.

After successfully moving the file, the function creates appropriate archive status files and signals relevant processes about the availability of new WAL data.

## Parameters / Member Variables
- : The temporary path where the restored file currently exists
- : The target filename in pg_wal directory (without directory path)

## Dependencies
- Functions called/Symbols referenced:
  - rename: Renames files (Windows-specific temporary renaming)
  - [strlcpy](../s/strlcpy.md): Safe string copying (non-Windows platforms)
  - unlink: Removes old files
  - [durable_rename](../d/durable_rename.md): Performs atomic file rename with fsync
  - [XLogArchiveForceDone](../X/XLogArchiveForceDone.md): Creates .done file when not in ALWAYS archive mode
  - [XLogArchiveNotify](../X/XLogArchiveNotify.md): Creates .ready file in ALWAYS archive mode
  - [WalSndRqstFileReload](../W/WalSndRqstFileReload.md): Requests WAL senders to reload the current segment
  - [WalSndWakeup](../W/WalSndWakeup.md): Signals WAL senders about new WAL availability
- Called from (representative examples):
  - [XLogFileRead](../X/XLogFileRead.md): After successfully restoring a WAL file during recovery
  - [restoreTimeLineHistoryFiles](../r/restoreTimeLineHistoryFiles.md): When keeping restored timeline history files
  - [readTimeLineHistory](../r/readTimeLineHistory.md): During timeline history file processing

## Notes and Other Information
- Implements Windows-specific workarounds for file locking issues using unique deletion counters
- Handles archive mode appropriately - creates .done files in non-ALWAYS mode, .ready files in ALWAYS mode
- Ensures WAL senders are properly notified about file changes through reload requests and wakeup signals
- Uses durable_rename to ensure atomic file replacement with proper synchronization
- Critical for maintaining consistency between restored files and active WAL processing
- The reload mechanism is essential for preventing WAL senders from serving stale data after file replacement

## Simplified Source

```c
// Simplified version of KeepFileRestoredFromArchive
void KeepFileRestoredFromArchive(const char *path, const char *xlogfname) {
    char xlogfpath[MAXPGPATH];
    bool reload = false;
    struct stat statbuf;

    // Construct target path in pg_wal directory
    snprintf(xlogfpath, MAXPGPATH, XLOGDIR "/%s", xlogfname);

    // Handle existing file replacement
    if (stat(xlogfpath, &statbuf) == 0) {
        char oldpath[MAXPGPATH];

#ifdef WIN32
        // Windows: rename old file to avoid locking issues
        static unsigned int deletedcounter = 1;
        snprintf(oldpath, MAXPGPATH, "%s.deleted%u", xlogfpath, deletedcounter++);
        if (rename(xlogfpath, oldpath) != 0)
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not rename file \"%s\" to \"%s\": %m",
                                  xlogfpath, oldpath)));
#else
        // Non-Windows: use original path
        strlcpy(oldpath, xlogfpath, MAXPGPATH);
#endif
        // Remove old file
        if (unlink(oldpath) != 0)
            ereport(FATAL, (errcode_for_file_access(),
                           errmsg("could not remove file \"%s\": %m", xlogfpath)));
        reload = true;
    }

    // Move restored file to permanent location
    durable_rename(path, xlogfpath, ERROR);

    // Create archive status file based on archive mode
    if (XLogArchiveMode != ARCHIVE_MODE_ALWAYS)
        XLogArchiveForceDone(xlogfname);  // Create .done file
    else
        XLogArchiveNotify(xlogfname);     // Create .ready file

    // Notify WAL senders if file was replaced
    if (reload)
        WalSndRqstFileReload();

    // Signal new WAL availability
    WalSndWakeup(true, false);
}
```

Key simplifications made:
- Maintained platform-specific file handling for Windows vs non-Windows
- Preserved essential error handling and cleanup logic
- Simplified complex path construction and file management
- Kept WAL sender notification and archive mode handling intact