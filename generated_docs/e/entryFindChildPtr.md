# entryFindChildPtr

## Location
[src/backend/access/gin/ginentrypage.c:405-445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L405-L445)

## Overview
Finds the offset of a child pointer (downlink) to a specific block number within a non-leaf GIN index page, using optimization hints from previously stored offset information.

## Definition

```c
static OffsetNumber
entryFindChildPtr(GinBtree btree, Page page, BlockNumber blkno, OffsetNumber storedOff)
```
## Detailed Description
This function searches for a child pointer (downlink) to a specific block number within a non-leaf page of a GIN index. It employs a multi-stage search strategy for efficiency:

1. First, it checks if the previously stored offset still points to the correct child pointer, which would be the case if the page hasn't been modified.
2. If the stored offset is invalid, it searches to the right of the stored position, optimizing for the common case where entries are inserted rather than deleted.
3. As a last resort, it performs a complete linear scan of the page from the beginning.

The function is designed to handle page modifications that might have occurred since the offset was stored, such as insertions or deletions that could shift child pointers to different positions.

## Parameters / Member Variables
- `btree`: GinBtree structure (currently unused in function body)
- `page`: The non-leaf page to search within
- `blkno`: The block number of the child page being searched for
- `storedOff`: Previously stored offset number that might point to the target child pointer, used as optimization hint
## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - GinPageIsLeaf
  - GinPageIsData
  - FirstOffsetNumber
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - GinGetDownlink
  - InvalidOffsetNumber
- Called from (representative examples):
  - [ginPrepareEntryScan](../g/ginPrepareEntryScan.md)

## Notes and Other Information
- This is a static function internal to the GIN entry page implementation
- The function assumes the input page is a non-leaf, non-data page (verified by assertions)
- Uses a three-phase search strategy: stored position check, rightward search, complete scan
- Returns InvalidOffsetNumber if the child pointer is not found on the page
- Optimized for the common case where page modifications involve insertions rather than deletions
- Critical for maintaining parent-child relationships during index traversal after page splits or modifications

## Simplified Source

```c
// Simplified version of entryFindChildPtr
static OffsetNumber entryFindChildPtr(GinBtree btree, Page page, BlockNumber blkno, OffsetNumber storedOff) {
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

    // Try the stored offset first (optimization for unchanged pages)
    if (storedOff >= FirstOffsetNumber && storedOff <= maxoff) {
        IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, storedOff));
        if (GinGetDownlink(itup) == blkno)
            return storedOff;

        // Search rightward from stored position (common case for insertions)
        for (OffsetNumber i = storedOff + 1; i <= maxoff; i++) {
            itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
            if (GinGetDownlink(itup) == blkno)
                return i;
        }
        maxoff = storedOff - 1;
    }

    // Last resort: complete linear scan from beginning
    for (OffsetNumber i = FirstOffsetNumber; i <= maxoff; i++) {
        IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
        if (GinGetDownlink(itup) == blkno)
            return i;
    }

    return InvalidOffsetNumber;
}
```

Key simplifications made:
- Removed assertions for clarity
- Added explanatory comments for each search phase
- Consolidated variable declarations
- Emphasized the three-phase search strategy
- Maintained the optimization logic while improving readability