# XLogBytePosToEndRecPtr

## Location
[src/backend/access/transam/xlog.c:1900-1942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L1900-L1942)

## Overview
Converts a usable byte position to an XLogRecPtr with special handling for page boundaries, returning pointers to page starts when at boundaries.

## Definition
static XLogRecPtr XLogBytePosToEndRecPtr(uint64 bytepos)

## Detailed Description
XLogBytePosToEndRecPtr is similar to XLogBytePosToRecPtr but provides special handling for positions that fall exactly at page boundaries. When the byte position corresponds to a page boundary, this function returns a pointer to the beginning of the page (before the page header) rather than to where the first WAL record on that page would be located.

This behavior is specifically designed for converting pointers that represent the end of a WAL record. When a record ends exactly at a page boundary, the "end" should point to the page boundary itself, not to the location where the next record would start (which would be after the page header).

The function handles both the first page of a segment (with long headers) and subsequent pages (with short headers), accounting for the different header sizes in the conversion calculations.

## Parameters / Member Variables
- : The usable byte position (excluding WAL page headers) to convert, typically representing the end position of a WAL record

## Dependencies
- Functions called/Symbols referenced:
  - SizeOfXLogLongPHD
  - UsableBytesInPage
  - SizeOfXLogShortPHD
  - XLogSegNoOffsetToRecPtr
- Called from (representative examples):
  - RefreshXLogWriteResult
  - [ReserveXLogInsertLocation](../R/ReserveXLogInsertLocation.md)
  - [ReserveXLogSwitch](../R/ReserveXLogSwitch.md)
  - [WaitXLogInsertionsToFinish](../W/WaitXLogInsertionsToFinish.md)

## Notes and Other Information
- This is a static function internal to xlog.c
- Key difference from XLogBytePosToRecPtr: special handling when bytesleft == 0 (page boundary conditions)
- Critical for accurately representing WAL record end positions that align with page boundaries
- Used in conjunction with WAL insertion and writing operations where precise end-of-record positioning is important
- The boundary handling ensures that record end positions are correctly represented in the WAL addressing scheme

## Simplified Source

```c
// Simplified version of XLogBytePosToEndRecPtr
static XLogRecPtr XLogBytePosToEndRecPtr(uint64 bytepos) {
    uint64 fullsegs;
    uint64 fullpages;
    uint64 bytesleft;
    uint32 seg_offset;
    XLogRecPtr result;

    // Calculate which segment and remaining bytes
    fullsegs = bytepos / UsableBytesInSegment;
    bytesleft = bytepos % UsableBytesInSegment;

    if (bytesleft < XLOG_BLCKSZ - SizeOfXLogLongPHD) {
        // Position fits on first page of segment (has long header)
        if (bytesleft == 0) {
            seg_offset = 0;  // At page boundary - return page start
        } else {
            seg_offset = bytesleft + SizeOfXLogLongPHD;
        }
    } else {
        // Position is on subsequent pages (have short headers)
        seg_offset = XLOG_BLCKSZ;  // Skip first page
        bytesleft -= XLOG_BLCKSZ - SizeOfXLogLongPHD;

        fullpages = bytesleft / UsableBytesInPage;
        bytesleft = bytesleft % UsableBytesInPage;

        if (bytesleft == 0) {
            // At page boundary - point to page start
            seg_offset += fullpages * XLOG_BLCKSZ;
        } else {
            // Within page - add header size
            seg_offset += fullpages * XLOG_BLCKSZ + bytesleft + SizeOfXLogShortPHD;
        }
    }

    // Convert segment number and offset to XLogRecPtr
    XLogSegNoOffsetToRecPtr(fullsegs, seg_offset, wal_segment_size, result);
    return result;
}
```

Key simplifications made:
- Added comments explaining the page boundary special handling
- Clarified the difference between long and short page headers
- Simplified the logic flow with clearer comments about when we're at boundaries
- Maintained all essential calculations for proper WAL addressing
- Preserved the critical boundary condition handling that differentiates this from XLogBytePosToRecPtr