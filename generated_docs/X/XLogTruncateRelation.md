# XLogTruncateRelation

## Location
src/backend/access/transam/xlogutils.c: 671 - 717

## Overview
Cleans up invalid page records for truncated pages during XLOG replay when a relation is being truncated.

## Definition
```c
void XLogTruncateRelation(RelFileLocator rlocator, ForkNumber forkNum, BlockNumber nblocks)
```

## Detailed Description
This function is called during WAL replay when a relation truncation operation is being replayed. When a relation is truncated, pages beyond the new size become invalid and any "invalid-page" records tracking those pages must be cleaned up to prevent memory leaks and maintain consistency.

The function delegates the cleanup to forget_invalid_pages(), passing the relation locator, fork number, and the new block count. This ensures that any invalid page records for blocks at or beyond the specified nblocks position are removed from the invalid page tracking data structures.

## Parameters / Member Variables
- `rlocator`: RelFileLocator identifying the relation being truncated (contains database OID, tablespace OID, and relation number)
- `forkNum`: ForkNumber specifying which fork of the relation is being truncated (main, fsm, vm, etc.)
- `nblocks`: BlockNumber indicating the new size of the relation in blocks - pages at this position and beyond are considered dropped

## Dependencies
- Functions called/Symbols referenced:
  - [forget_invalid_pages](../f/forget_invalid_pages.md)

- Called from (representative examples):
  - [smgr_redo](../s/smgr_redo.md)
  - InHotStandby (referenced in header)

## Notes and Other Information
- This function is specifically designed for use during XLOG replay of relation truncation operations
- Essential for maintaining consistency in invalid page tracking when relations are truncated during recovery
- The nblocks parameter represents the new size limit - all pages at position nblocks and higher are considered truncated
- Part of the WAL replay infrastructure that ensures proper cleanup of tracking data structures during relation modifications
- Helps prevent memory leaks by removing obsolete invalid page records for truncated portions of relations
- Works in conjunction with the broader invalid page tracking system to maintain database consistency during recovery operations