# mdtruncate

## Location
src/backend/storage/smgr/md.c: 1153 - 1241

## Overview
mdtruncate truncates a PostgreSQL relation to a specified number of blocks, handling multi-segment files and ensuring proper resource management without memory allocation.

## Definition


## Detailed Description
mdtruncate safely reduces the size of a PostgreSQL relation by truncating it to the specified number of blocks. The function is designed to be memory-allocation-free, making it safe for use in critical sections. It operates by truncating segments starting from the last one, which simplifies memory management for the file descriptor array in case of errors.

The function handles three scenarios for each segment: complete removal (truncate to 0 but keep the file), partial truncation (truncate to exact size needed), or preservation (no action needed). It maintains PostgreSQL's invariant of keeping at least the first segment and handles the special case where nblocks is exactly a multiple of RELSEG_SIZE by keeping a zero-length final segment.

## Parameters / Member Variables
- : SMgrRelation pointer representing the relation to truncate
- : ForkNumber identifying which fork of the relation to truncate
- : BlockNumber indicating the current number of blocks (must be obtained while holding appropriate locks)
- : BlockNumber specifying the target number of blocks after truncation

## Dependencies
- Functions called/Symbols referenced:
  - relpath
  - FileTruncate
  - [FilePathName](../F/FilePathName.md)
  - SmgrIsTemp
  - [register_dirty_segment](../r/register_dirty_segment.md)
  - FileClose
  - [_fdvec_resize](../f/_fdvec_resize.md)
- Called from (representative examples):
  - Storage manager layer functions (via MD_H interface)

## Notes and Other Information
- Guaranteed not to allocate memory, making it safe for critical sections
- Requires that smgrnblocks() was called beforehand to ensure all segments are opened
- Caller must hold sufficient locks to prevent concurrent relation size changes
- Processes segments from last to first to simplify error handling
- Truncates rather than deletes inactive segment files for safety reasons
- Maintains the invariant that the first segment is never dropped
- Handles the special case where nblocks is a multiple of RELSEG_SIZE by keeping a zero-length segment
- Registers dirty segments for fsync unless the relation is temporary
- Includes validation to prevent truncation beyond current size (except during recovery)
- Uses proper error reporting with file paths and block counts for debugging