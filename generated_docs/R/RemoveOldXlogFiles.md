# RemoveOldXlogFiles

## Location
[src/backend/access/transam/xlog.c:3842-3916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L3842-L3916)

## Overview
Recycles or removes WAL log files that are older than or equal to a specified segment number, helping manage disk space while ensuring required WAL segments for recovery are preserved.

## Definition

```c
struct dirent *xlde;
```
## Detailed Description
This function manages the lifecycle of WAL files by removing or recycling segments that are no longer needed for recovery. It intelligently decides between recycling (reusing) and deletion based on various factors including the current WAL position and the last checkpoint's redo pointer. 

The function operates by:
1. Calculating recycling boundaries using the end pointer and redo pointer
2. Constructing the filename of the last segment to be preserved
3. Scanning the pg_wal directory for eligible files
4. For each WAL segment older than the threshold, checking if it has been archived
5. Updating shared memory tracking before removal
6. Calling RemoveXlogFile to handle the actual recycling or deletion

The algorithm preserves segments from parent timelines to avoid premature removal and uses alphanumeric filename sorting to determine segment age.

## Parameters / Member Variables
- : The segment number threshold - files older than or equal to this will be considered for removal
- : The redo pointer from the last checkpoint, used to determine recycling vs deletion strategy  
- : Current or recent end of WAL, used for recycling calculations
- : The current timeline ID for XLOG insertion - recycled segments will be reused for this timeline

## Dependencies
- Functions called/Symbols referenced:
  - XLByteToSeg
  - [XLOGfileslop](../X/XLOGfileslop.md)
  - [XLogFileName](../X/XLogFileName.md)
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDir](ReadDir.md)
  - [FreeDir](../F/FreeDir.md)
  - [IsXLogFileName](../I/IsXLogFileName.md)
  - [IsPartialXLogFileName](../I/IsPartialXLogFileName.md)
  - [XLogArchiveCheckDone](../X/XLogArchiveCheckDone.md)
  - [UpdateLastRemovedPtr](../U/UpdateLastRemovedPtr.md)
  - [RemoveXlogFile](RemoveXlogFile.md)
  - elog
- Constants used:
  - XLOGDIR
  - DEBUG2
  - MAXFNAMELEN
- Called from:
  - [CreateCheckPoint](../C/CreateCheckPoint.md)
  - [CreateRestartPoint](../C/CreateRestartPoint.md)
  - RefreshXLogWriteResult

## Notes and Other Information
- This function is declared as static, meaning it's only accessible within the xlog.c file
- The timeline portion of WAL segment names is ignored during comparison to prevent premature removal of parent timeline segments
- Files are only removed after confirming they have been successfully archived (via XLogArchiveCheckDone)
- The function updates shared memory tracking (via UpdateLastRemovedPtr) before actual file removal
- Alphanumeric sorting of filenames is used to determine segment chronological order
- File path: src/backend/access/transam/xlog.c:3842-3916

## Simplified Source

```c
// Simplified version of RemoveOldXlogFiles
static void
RemoveOldXlogFiles(XLogSegNo segno, XLogRecPtr lastredoptr, XLogRecPtr endptr,
                   TimeLineID insertTLI)
{
    DIR *xldir;
    struct dirent *xlde;
    char lastoff[MAXFNAMELEN];
    XLogSegNo endlogSegNo;
    XLogSegNo recycleSegNo;

    // Calculate recycling boundaries
    XLByteToSeg(endptr, endlogSegNo, wal_segment_size);
    recycleSegNo = XLOGfileslop(lastredoptr);

    // Create filename of last segment to keep (timeline ignored)
    XLogFileName(lastoff, 0, segno, wal_segment_size);

    // Open WAL directory for scanning
    xldir = AllocateDir(XLOGDIR);

    // Scan directory for WAL files to remove
    while ((xlde = ReadDir(xldir, XLOGDIR)) != NULL)
    {
        // Skip non-WAL files
        if (!IsXLogFileName(xlde->d_name) && !IsPartialXLogFileName(xlde->d_name))
            continue;

        // Check if file is older than threshold (using alphanumeric comparison)
        if (strcmp(xlde->d_name + 8, lastoff + 8) <= 0)
        {
            // Only remove if archiving is complete
            if (XLogArchiveCheckDone(xlde->d_name))
            {
                // Update shared memory tracking
                UpdateLastRemovedPtr(xlde->d_name);

                // Remove or recycle the file
                RemoveXlogFile(xlde, recycleSegNo, &endlogSegNo, insertTLI);
            }
        }
    }

    // Clean up directory handle
    FreeDir(xldir);
}
```

Key simplifications made:
- Removed detailed comments explaining timeline handling strategy
- Consolidated debug logging (removed elog call)
- Simplified variable declarations and initialization
- Focused on the main execution path: scan → filter → archive check → remove
- Abstracted the complex recycling logic into the RemoveXlogFile call
- Maintained the essential algorithm while reducing cognitive overhead