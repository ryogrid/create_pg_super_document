# XLogDropRelation

## Location
[src/backend/access/transam/xlogutils.c:641-651](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L641-L651)

## Overview
Cleans up invalid page records for a relation during XLOG replay when the relation is about to be deleted.

## Definition
```c
void XLogDropRelation(RelFileLocator rlocator, ForkNumber forknum)
```

## Detailed Description
This function is called during WAL replay when a relation is about to be deleted. Its primary purpose is to clean up any "invalid-page" records that may exist for the relation being dropped. Invalid page records are maintained during recovery to track pages that may contain stale or inconsistent data.

When a relation is dropped, any invalid page tracking for that relation becomes obsolete and must be removed to prevent memory leaks and avoid confusion in subsequent recovery operations. The function delegates this cleanup to forget_invalid_pages() with a block number of 0, indicating that all invalid page records for the specified relation and fork should be removed.

## Parameters / Member Variables
- `rlocator`: RelFileLocator identifying the relation being dropped (contains database OID, tablespace OID, and relation number)
- `forknum`: ForkNumber specifying which fork of the relation is being dropped (main, fsm, vm, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [forget_invalid_pages](../f/forget_invalid_pages.md)

- Called from (representative examples):
  - [DropRelationFiles](../D/DropRelationFiles.md)
  - InHotStandby (referenced in header)

## Notes and Other Information
- This function is specifically designed for use during XLOG replay, not during normal database operations
- The function ensures that invalid page tracking data structures remain clean and do not accumulate stale entries for deleted relations
- Called with block number 0 in forget_invalid_pages(), which typically means "forget all invalid pages for this relation/fork"
- Essential for proper memory management during recovery operations
- Part of the WAL replay infrastructure that maintains consistency between the primary and standby servers

## Simplified Source

```c
void XLogDropRelation(RelFileLocator rlocator, ForkNumber forknum) {
    // Clean up invalid page records for the relation being dropped
    forget_invalid_pages(rlocator, forknum, 0);
}
```