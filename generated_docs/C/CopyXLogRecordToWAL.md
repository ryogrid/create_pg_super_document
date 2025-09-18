# CopyXLogRecordToWAL

## Location
src/backend/access/transam/xlog.c: 1227 - 1372

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
  - GetXLogBuffer
  - INSERT_FREESPACE
  - XLogSegmentOffset
  - memcpy
  - MemSet
  - MAXALIGN64
  - ereport
- Called from (representative examples):
  - XLogInsertRecord
  - RefreshXLogWriteResult

## Notes and Other Information
- Handles multi-page records by setting continuation record flags (XLP_FIRST_IS_CONTRECORD) appropriately
- For xlog-switch records, zeros out remaining space in the segment to improve compression ratios
- Uses different page header sizes (SizeOfXLogLongPHD vs SizeOfXLogShortPHD) depending on segment position
- Includes panic-level error checking to ensure written data matches the expected length
- The function must match exactly with the space calculations in ReserveXLogInsertLocation
- Optimizes xlog-switch padding by zeroing page headers to improve compression (especially with tools like bzip2)
- Aligns the final position to ensure the next record starts at a proper boundary