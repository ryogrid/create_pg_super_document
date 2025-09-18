# XLogBytePosToEndRecPtr

## Location
src/backend/access/transam/xlog.c: 1900 - 1942

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