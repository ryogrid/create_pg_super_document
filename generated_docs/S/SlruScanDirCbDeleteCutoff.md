# SlruScanDirCbDeleteCutoff

## Location
[src/backend/access/transam/slru.c:1725-1740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1725-L1740)

## Overview
A static callback function used by SlruScanDirectory to delete SLRU segments that are prior to a specified cutoff page.

## Definition
```c
static bool SlruScanDirCbDeleteCutoff(SlruCtl ctl, char *filename, int64 segpage, void *data)
```

## Detailed Description
SlruScanDirCbDeleteCutoff is a static callback function that implements the actual deletion logic for SLRU segments during truncation operations. Unlike SlruScanDirCbReportPresence which only reports presence, this callback performs the actual deletion of segments that meet the cutoff criteria. It works by receiving a cutoff page number through the data parameter, checking each segment via SlruMayDeleteSegment, and if the segment is eligible for deletion, calling SlruInternalDeleteSegment to remove it from the filesystem. The function always returns false to continue scanning all segments in the directory.

## Parameters / Member Variables
- `ctl`: SlruCtl structure containing SLRU control information and configuration
- `filename`: Name of the SLRU segment file being examined (unused in this callback)
- `segpage`: Page number representing the first page of the segment being examined
- `data`: Void pointer to the cutoff page number (cast to int64)

## Dependencies
- Functions called/Symbols referenced:
  - [SlruMayDeleteSegment](SlruMayDeleteSegment.md)
  - [SlruInternalDeleteSegment](SlruInternalDeleteSegment.md)  
  - SLRU_PAGES_PER_SEGMENT
- Called from (representative examples):
  - [SimpleLruTruncate](SimpleLruTruncate.md)

## Notes and Other Information
- Always returns false to ensure all segments in the directory are processed
- Uses SLRU_PAGES_PER_SEGMENT to convert page numbers to segment numbers for deletion
- Part of the SLRU truncation mechanism used to clean up old transaction log data
- Static function, only accessible within the slru.c compilation unit