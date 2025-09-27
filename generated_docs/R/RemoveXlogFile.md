# RemoveXlogFile

## Location
[src/backend/access/transam/xlog.c:3986-4075](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L3986-L4075)

## Overview
Handles the actual recycling or removal of a single WAL log file, intelligently deciding between reuse for future segments and permanent deletion based on system needs and constraints.

## Definition

```c
static void
RemoveXlogFile(const struct dirent *segment_de,
			   XLogSegNo recycleSegNo, XLogSegNo *endlogSegNo,
			   TimeLineID insertTLI)
```
## Detailed Description
This function performs the low-level work of either recycling or removing a WAL segment file. It implements an intelligent recycling strategy where segments can be reused as future WAL files if conditions are met, otherwise they are permanently deleted.

The recycling decision process considers:
1. Whether WAL recycling is enabled (wal_recycle setting)
2. Whether the current end log segment is within recycling range
3. Whether InstallXLogFileSegment is currently active
4. Whether the file is a regular file (not a symbolic link)
5. Whether InstallXLogFileSegment succeeds

If recycling fails or isn't appropriate, the file is deleted. On Windows, the function handles a special case where files may need to be renamed before deletion to avoid confusion with lingering deleted files.

The function also updates checkpoint statistics and triggers WAL archive cleanup.

## Parameters / Member Variables
- : Directory entry structure for the segment to process
- : Upper bound segment number for recycling eligibility
- : Pointer to current/recent end of WAL segment number (incremented if recycled)
- : Current timeline ID for XLOG insertion - recycled segments will use this timeline

## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - [get_dirent_type](../g/get_dirent_type.md)
  - [InstallXLogFileSegment](../I/InstallXLogFileSegment.md)
  - ereport
  - rename (Windows only)
  - [durable_unlink](../d/durable_unlink.md)
  - [XLogArchiveCleanup](../X/XLogArchiveCleanup.md)
- Constants used:
  - XLOGDIR
  - MAXPGPATH
  - PGFILETYPE_REG
  - DEBUG2
  - LOG
- Global variables:
  - wal_recycle
  - XLogCtl->InstallXLogFileSegmentActive
  - CheckpointStats
- Called from:
  - [RemoveOldXlogFiles](RemoveOldXlogFiles.md)
  - [RemoveNonParentXlogFiles](RemoveNonParentXlogFiles.md)
  - RefreshXLogWriteResult

## Notes and Other Information
- This function is declared as static, limiting access to within xlog.c
- Recycling is preferred over deletion when possible to reduce I/O overhead of creating new WAL segments
- Windows-specific logic handles file sharing issues by renaming before deletion
- Only regular files are candidates for recycling (symbolic links are always deleted)
- Checkpoint statistics are updated to track both recycled and removed segments
- The function triggers archive cleanup for the removed segment
- File path: src/backend/access/transam/xlog.c:3986-4075

## Simplified Source

```c
// Simplified version of RemoveXlogFile
static void RemoveXlogFile(const struct dirent *segment_de,
                          XLogSegNo recycleSegNo, XLogSegNo *endlogSegNo,
                          TimeLineID insertTLI) {
    char path[MAXPGPATH];
    const char *segname = segment_de->d_name;

    // Build the full path to the WAL segment
    snprintf(path, MAXPGPATH, XLOGDIR "/%s", segname);

    // Try to recycle the segment first if conditions are met
    if (wal_recycle &&
        *endlogSegNo <= recycleSegNo &&
        XLogCtl->InstallXLogFileSegmentActive &&
        get_dirent_type(path, segment_de, false, DEBUG2) == PGFILETYPE_REG &&
        InstallXLogFileSegment(endlogSegNo, path, true, recycleSegNo, insertTLI)) {

        // Successfully recycled the segment
        CheckpointStats.ckpt_segs_recycled++;
        (*endlogSegNo)++;
    } else {
        // Recycling failed or not appropriate, remove the file
        int rc;

#ifdef WIN32
        // On Windows, rename before deletion to avoid file sharing issues
        char newpath[MAXPGPATH];
        snprintf(newpath, MAXPGPATH, "%s.deleted", path);
        if (rename(path, newpath) != 0) {
            return;  // Retry at next checkpoint
        }
        rc = durable_unlink(newpath, LOG);
#else
        rc = durable_unlink(path, LOG);
#endif

        if (rc == 0) {
            CheckpointStats.ckpt_segs_removed++;
        }
    }

    // Clean up any archive references to this segment
    XLogArchiveCleanup(segname);
}
```

Key simplifications made:
- Removed detailed error reporting and debug messages for clarity
- Consolidated the recycling vs removal decision logic
- Simplified Windows-specific file handling while preserving the core behavior
- Focused on the main algorithm: try recycling first, then remove if necessary
- Preserved essential statistics tracking and cleanup operations