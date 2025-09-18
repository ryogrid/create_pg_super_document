# moveRightIfItNeeded

## Location
src/backend/access/gin/ginget.c: 43 - 68

## Overview
This is a static helper function in the GIN (Generalized Inverted Index) access method that handles page navigation by moving to the next page when the current scan position exceeds the bounds of the current page.

## Definition
```c
static bool moveRightIfItNeeded(GinBtreeData *btree, GinBtreeStack *stack, Snapshot snapshot)
```

## Detailed Description
The function checks if the current offset in the GIN btree stack has exceeded the maximum offset number of the current page. If so, it navigates to the next page on the right in the btree structure. This is part of the GIN index scanning mechanism that ensures continuous traversal across multiple pages during index operations.

The function handles the boundary condition when scanning reaches the end of a page and needs to continue on the next page. It updates the stack's buffer, block number, and offset accordingly, and applies appropriate locking for the new page.

## Parameters / Member Variables
- `btree`: Pointer to GinBtreeData structure containing btree metadata and index information
- `stack`: Pointer to GinBtreeStack structure representing the current position in the btree traversal
- `snapshot`: Snapshot for MVCC consistency and predicate locking

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - GinPageRightMost
  - [ginStepRight](../g/ginStepRight.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PredicateLockPage](../P/PredicateLockPage.md)
  - GIN_SHARE (lock mode constant)
  - FirstOffsetNumber (constant)
- Called from:
  - [collectMatchBitmap](../c/collectMatchBitmap.md) (src/backend/access/gin/ginget.c:157, 268)

## Notes and Other Information
- This is a static function, only accessible within the ginget.c file
- Returns `false` when reaching the rightmost page (no more pages to scan), `true` otherwise
- Part of the GIN index scanning infrastructure that supports efficient bitmap collection during index searches
- Handles predicate locking for the newly accessed page to maintain proper isolation levels
- The function is critical for maintaining scan continuity across page boundaries in GIN indexes