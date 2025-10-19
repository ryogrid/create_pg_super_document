# SimpleXLogPageRead

## Location
[src/bin/pg_rewind/parsexlog.c:275-388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/parsexlog.c#L275-L388)

## Overview
A callback function for XLogReader that handles reading WAL pages from disk, including timeline switching and archive recovery functionality for pg_rewind operations.

## Definition

```c
static int
SimpleXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr,
				   int reqLen, XLogRecPtr targetRecPtr, char *readBuf)
```
## Detailed Description
This function serves as the page reading callback for XLogReader operations in pg_rewind. It handles the complex task of reading WAL pages from the appropriate timeline and segment files, including automatic timeline switching when crossing timeline boundaries and archive recovery when local WAL files are not available.

The function manages file descriptor state for the currently open WAL segment, automatically closing and reopening files when switching between segments. It implements timeline switching logic by examining the target timeline history to determine the correct timeline for the requested WAL position. When a WAL file is not found locally, it can optionally attempt to restore it from archives using the provided restore command.

The function handles various error conditions gracefully and provides detailed logging for debugging purposes. It ensures that exactly XLOG_BLCKSZ bytes are read and updates the reader state with the appropriate timeline information.

## Parameters / Member Variables
- `*xlogreader`: XLogReaderState containing reader context and private data
- `targetPagePtr`: XLogRecPtr indicating the WAL page position to read
- `reqLen`: Requested length (typically ignored, reads full XLOG_BLCKSZ pages)
- `targetRecPtr`: XLogRecPtr of the target record (used for context)
- `*readBuf`: Buffer to store the read WAL page data
## Dependencies
- Functions called/Symbols referenced:
  - XLByteToSeg
  - XLogSegNoOffsetToRecPtr
  - XLogSegmentOffset
  - XLByteInSeg
  - [XLogFileName](../X/XLogFileName.md)
  - [RestoreArchivedFile](../R/RestoreArchivedFile.md)
  - open
  - close
  - lseek
  - read
  - pg_log_error
  - pg_log_debug
  - [XLogPageReadPrivate](../X/XLogPageReadPrivate.md)
- Called from (representative examples):
  - [extractPageMap](../e/extractPageMap.md) (via XL_ROUTINE callback)
  - [readOneRecord](../r/readOneRecord.md) (via XL_ROUTINE callback)  
  - [findLastCheckpoint](../f/findLastCheckpoint.md) (via XL_ROUTINE callback)

## Notes and Other Information
- Returns XLOG_BLCKSZ on success, -1 on failure
- Implements automatic timeline switching based on targetHistory array
- Supports archive recovery through restore_command when local files are missing
- Manages global xlogreadfd file descriptor for the currently open segment
- Updates xlogreader->seg.ws_tli to reflect the timeline of the read page
- Critical component that enables pg_rewind to read WAL across timeline boundaries
- Uses pg_rewind-specific XLogPageReadPrivate structure for timeline context

## Simplified Source

```c
static int SimpleXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr,
                             int reqLen, XLogRecPtr targetRecPtr, char *readBuf)
{
    XLogPageReadPrivate *private = (XLogPageReadPrivate *) xlogreader->private_data;
    uint32 targetPageOff;
    XLogSegNo targetSegNo;

    // Calculate target segment and page offset
    XLByteToSeg(targetPagePtr, targetSegNo, WalSegSz);
    targetPageOff = XLogSegmentOffset(targetPagePtr, WalSegSz);

    // Close current file if we need to switch to different segment
    if (xlogreadfd >= 0 && !XLByteInSeg(targetPagePtr, xlogreadsegno, WalSegSz)) {
        close(xlogreadfd);
        xlogreadfd = -1;
    }

    XLByteToSeg(targetPagePtr, xlogreadsegno, WalSegSz);

    // Open new WAL segment file if needed
    if (xlogreadfd < 0) {
        char xlogfname[MAXFNAMELEN];

        // Switch to correct timeline for this segment
        while (private->tliIndex < targetNentries - 1 &&
               targetHistory[private->tliIndex].end < targetSegEnd)
            private->tliIndex++;
        while (private->tliIndex > 0 &&
               targetHistory[private->tliIndex].begin >= targetSegEnd)
            private->tliIndex--;

        // Build filename and path for WAL segment
        XLogFileName(xlogfname, targetHistory[private->tliIndex].tli,
                    xlogreadsegno, WalSegSz);
        snprintf(xlogfpath, MAXPGPATH, "%s/" XLOGDIR "/%s",
                xlogreader->segcxt.ws_dir, xlogfname);

        // Try to open local file
        xlogreadfd = open(xlogfpath, O_RDONLY | PG_BINARY, 0);

        // If local file not found, try archive recovery
        if (xlogreadfd < 0 && private->restoreCommand != NULL) {
            xlogreadfd = RestoreArchivedFile(xlogreader->segcxt.ws_dir,
                                           xlogfname, WalSegSz,
                                           private->restoreCommand);
        }

        if (xlogreadfd < 0)
            return -1;
    }

    // Read the requested WAL page
    if (lseek(xlogreadfd, (off_t) targetPageOff, SEEK_SET) < 0) {
        pg_log_error("could not seek in file \"%s\": %m", xlogfpath);
        return -1;
    }

    int r = read(xlogreadfd, readBuf, XLOG_BLCKSZ);
    if (r != XLOG_BLCKSZ) {
        pg_log_error("could not read file \"%s\"", xlogfpath);
        return -1;
    }

    // Update reader state with timeline info
    xlogreader->seg.ws_tli = targetHistory[private->tliIndex].tli;
    return XLOG_BLCKSZ;
}
```