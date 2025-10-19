# WALDumpReadPage

## Location
[src/bin/pg_waldump/pg_waldump.c:389-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L389-L437)

## Overview
WALDumpReadPage is a callback function used by the XLogReader infrastructure in pg_waldump to read WAL (Write-Ahead Log) pages from disk.

## Definition

```c
static int
WALDumpReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				XLogRecPtr targetPtr, char *readBuff)
```
## Detailed Description
This function serves as the page_read callback for the XLogReaderRoutine structure in pg_waldump. It handles reading WAL data pages from disk while respecting configured endpoint limits. The function manages partial reads when approaching the configured end position and provides detailed error reporting when read operations fail. It ensures that WAL data is read in complete XLOG_BLCKSZ-sized blocks when possible, or adjusts the read size when approaching the endpoint.

## Parameters / Member Variables
- `*state`: XLogReaderState containing the current reader state and private data
- `targetPagePtr`: XLogRecPtr indicating the WAL position of the page to read
- `reqLen`: Minimum number of bytes required to be read
- `targetPtr`: XLogRecPtr of the target record being read
- `*readBuff`: Buffer to store the read WAL data
## Dependencies
- Functions called/Symbols referenced:
  - [WALRead](WALRead.md)
  - [XLogFileName](../X/XLogFileName.md)
  - [XLogDumpPrivate](../X/XLogDumpPrivate.md) (type)
  - [WALReadError](WALReadError.md) (type)
  - [WALOpenSegment](WALOpenSegment.md) (type)
- Called from (representative examples):
  - [main](../m/main.md) (assigned as callback in XLogReaderRoutine)

## Notes and Other Information
- Returns the actual number of bytes read on success, or -1 when the configured endpoint is reached
- Handles endpoint checking to stop reading beyond the specified end position
- Provides comprehensive error reporting including file names, offsets, and system error messages
- Uses timeline information from private data for proper WAL segment identification
- Part of the pg_waldump utility's WAL reading infrastructure

## Simplified Source

```c
static int
WALDumpReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
                XLogRecPtr targetPtr, char *readBuff)
{
    XLogDumpPrivate *private = state->private_data;
    int count = XLOG_BLCKSZ;
    WALReadError errinfo;

    // Check if we've reached the configured endpoint
    if (private->endptr != InvalidXLogRecPtr) {
        if (targetPagePtr + XLOG_BLCKSZ <= private->endptr)
            count = XLOG_BLCKSZ;
        else if (targetPagePtr + reqLen <= private->endptr)
            count = private->endptr - targetPagePtr;
        else {
            // Mark endpoint reached and return
            private->endptr_reached = true;
            return -1;
        }
    }

    // Attempt to read WAL data
    if (!WALRead(state, readBuff, targetPagePtr, count, private->timeline, &errinfo)) {
        // Generate filename for error reporting
        char fname[MAXPGPATH];
        XLogFileName(fname, errinfo.wre_seg.ws_tli, errinfo.wre_seg.ws_segno,
                     state->segcxt.ws_segsize);

        // Report read failure with detailed error information
        if (errinfo.wre_errno != 0)
            pg_fatal("could not read from file \"%s\", offset %d: %m",
                     fname, errinfo.wre_off);
        else
            pg_fatal("could not read from file \"%s\", offset %d: read %d of %d",
                     fname, errinfo.wre_off, errinfo.wre_read, errinfo.wre_req);
    }

    return count;
}
```