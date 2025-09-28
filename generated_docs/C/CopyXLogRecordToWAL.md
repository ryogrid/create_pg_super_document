# CopyXLogRecordToWAL

## Location
[src/backend/access/transam/xlog.c:1227-1372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L1227-L1372)

## Overview
CopyXLogRecordToWAL is responsible for copying WAL record data from the provided XLogRecData chain to the already-reserved space in the WAL buffers, handling page boundaries and special xlog-switch padding.

## Definition
```c
static void CopyXLogRecordToWAL(int write_len, bool isLogSwitch, XLogRecData *rdata,
                               XLogRecPtr StartPos, XLogRecPtr EndPos, TimeLineID tli)
```

## Detailed Description
CopyXLogRecordToWAL performs the actual copying of WAL record data to the reserved WAL buffer space. It iterates through the XLogRecData chain, copying data while handling page boundaries within the WAL buffer. When a record spans multiple pages, it properly sets the XLP_FIRST_IS_CONTRECORD flag and xlp_rem_len field in subsequent page headers. For xlog-switch records, it also handles the special requirement of consuming all remaining space in the WAL segment by zeroing out the remainder of the segment for better compression. The function ensures proper alignment and validates that exactly the expected amount of data was written.

## Parameters / Member Variables
- `write_len`: Total length of data to be written to WAL
- `isLogSwitch`: Whether this is an xlog-switch record requiring special segment padding
- `rdata`: Chain of XLogRecData structures containing the data to copy
- `StartPos`: Starting position in WAL where copying should begin
- `EndPos`: Expected ending position after all data is copied
- `tli`: Timeline ID for the WAL buffers

## Dependencies
- Functions called/Symbols referenced:
  - [GetXLogBuffer](../G/GetXLogBuffer.md)
  - INSERT_FREESPACE
  - XLogSegmentOffset
  - memcpy
  - MemSet
  - MAXALIGN64
  - ereport
- Called from (representative examples):
  - [XLogInsertRecord](../X/XLogInsertRecord.md)
  - RefreshXLogWriteResult

## Notes and Other Information
- Handles multi-page records by setting continuation record flags (XLP_FIRST_IS_CONTRECORD) appropriately
- For xlog-switch records, zeros out remaining space in the segment to improve compression ratios
- Uses different page header sizes (SizeOfXLogLongPHD vs SizeOfXLogShortPHD) depending on segment position
- Includes panic-level error checking to ensure written data matches the expected length
- The function must match exactly with the space calculations in ReserveXLogInsertLocation
- Optimizes xlog-switch padding by zeroing page headers to improve compression (especially with tools like bzip2)
- Aligns the final position to ensure the next record starts at a proper boundary

## Simplified Source

```c
// Simplified version of CopyXLogRecordToWAL
static void CopyXLogRecordToWAL(int write_len, bool isLogSwitch, XLogRecData *rdata,
                               XLogRecPtr StartPos, XLogRecPtr EndPos, TimeLineID tli)
{
    char       *currpos;
    int         freespace;
    int         written = 0;
    XLogRecPtr  CurrPos;
    XLogPageHeader pagehdr;

    // Get starting position and available space in WAL buffer
    CurrPos = StartPos;
    currpos = GetXLogBuffer(CurrPos, tli);
    freespace = INSERT_FREESPACE(CurrPos);

    // Copy all record data from the linked list
    while (rdata != NULL) {
        char *data_ptr = rdata->data;
        int   data_len = rdata->len;

        // Handle data that spans multiple pages
        while (data_len > freespace) {
            // Copy what fits on current page
            memcpy(currpos, data_ptr, freespace);
            data_ptr += freespace;
            data_len -= freespace;
            written += freespace;
            CurrPos += freespace;

            // Move to next page and set continuation flags
            currpos = GetXLogBuffer(CurrPos, tli);
            pagehdr = (XLogPageHeader) currpos;
            pagehdr->xlp_rem_len = write_len - written;
            pagehdr->xlp_info |= XLP_FIRST_IS_CONTRECORD;

            // Skip page header (size depends on segment position)
            if (XLogSegmentOffset(CurrPos, wal_segment_size) == 0) {
                CurrPos += SizeOfXLogLongPHD;
                currpos += SizeOfXLogLongPHD;
            } else {
                CurrPos += SizeOfXLogShortPHD;
                currpos += SizeOfXLogShortPHD;
            }
            freespace = INSERT_FREESPACE(CurrPos);
        }

        // Copy remaining data that fits on current page
        memcpy(currpos, data_ptr, data_len);
        currpos += data_len;
        CurrPos += data_len;
        freespace -= data_len;
        written += data_len;

        rdata = rdata->next;
    }

    // Handle special case: xlog-switch record padding
    if (isLogSwitch && XLogSegmentOffset(CurrPos, wal_segment_size) != 0) {
        // Use remaining space on current page
        CurrPos += freespace;

        // Zero out all remaining pages in the segment for compression
        while (CurrPos < EndPos) {
            currpos = GetXLogBuffer(CurrPos, tli);
            MemSet(currpos, 0, SizeOfXLogShortPHD);
            CurrPos += XLOG_BLCKSZ;
        }
    } else {
        // Align end position for next record
        CurrPos = MAXALIGN64(CurrPos);
    }

    // Verify we wrote exactly what was expected
    if (CurrPos != EndPos) {
        ereport(PANIC, (errmsg_internal("WAL record size mismatch")));
    }
}
```

Key simplifications made:
- Removed detailed comments explaining low-level implementation details
- Simplified variable names and eliminated some intermediate calculations
- Consolidated assertion checks into essential validation at the end
- Abstracted complex page boundary logic into clearer flow structure
- Reduced verbose error messages while preserving essential error handling
- Maintained all core functionality: data copying, page spanning, continuation flags, and xlog-switch padding