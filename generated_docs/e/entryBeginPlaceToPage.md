# entryBeginPlaceToPage

## Location
[src/backend/access/gin/ginentrypage.c:527-553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L527-L553)

## Overview
Determines whether a tuple insertion can proceed on a GIN entry page or requires a page split, and initiates the appropriate action.

## Definition
```c
static GinPlaceToPageRC entryBeginPlaceToPage(GinBtree btree, Buffer buf, GinBtreeStack *stack,
                                             void *insertPayload, BlockNumber updateblkno,
                                             void **ptp_workspace,
                                             Page *newlpage, Page *newrpage)
```

## Detailed Description
entryBeginPlaceToPage is the decision-making function that prepares for tuple insertion on GIN entry pages. It follows a two-path approach:

1. **Space Check**: First evaluates whether there is sufficient space on the current page using entryIsEnoughSpace to accommodate the new entry.

2. **Split or Insert Decision**: 
   - If insufficient space exists, it triggers a page split operation via entrySplitPage, creating two new temporary page images (newlpage and newrpage) and returns GPTP_SPLIT.
   - If sufficient space exists, it simply returns GPTP_INSERT to indicate the insertion can proceed.

This function serves as the critical decision point in the GIN entry insertion process, determining the execution path before entering the critical section where actual modifications occur.

## Parameters / Member Variables
- `btree`: GinBtree structure containing B-tree context and configuration
- `buf`: Buffer containing the target page for insertion
- `stack`: GinBtreeStack representing the current position in the tree traversal
- `insertPayload`: Generic pointer to insertion data, cast to GinBtreeEntryInsertData internally
- `updateblkno`: Block number for updating downlinks in internal nodes (when child splits occur)
- `ptp_workspace`: Workspace pointer for passing information to subsequent execution phases (not used in this function)
- `newlpage`: Output pointer for left page image when splitting occurs
- `newrpage`: Output pointer for right page image when splitting occurs

## Dependencies
- Functions called/Symbols referenced:
  - [entryIsEnoughSpace](entryIsEnoughSpace.md)
  - [entrySplitPage](entrySplitPage.md)
  - [GinBtreeEntryInsertData](../G/GinBtreeEntryInsertData.md) (type casting)
  - GPTP_SPLIT
  - GPTP_INSERT
- Called from (representative examples):
  - [ginPrepareEntryScan](../g/ginPrepareEntryScan.md)

## Notes and Other Information
- This is a static function used internally within the GIN entry page management system
- The function does not modify the original page buffer - all modifications are deferred to later phases
- Part of the three-phase insertion pattern (begin/exec/finish) used throughout PostgreSQL index management
- The function specifically handles both leaf and internal node insertions, with internal nodes requiring downlink updates
- Return values follow the GinPlaceToPageRC enumeration to indicate the required action path

## Simplified Source

```c
static GinPlaceToPageRC
entryBeginPlaceToPage(GinBtree btree, Buffer buf, GinBtreeStack *stack,
                      void *insertPayload, BlockNumber updateblkno,
                      void **ptp_workspace,
                      Page *newlpage, Page *newrpage)
{
    GinBtreeEntryInsertData *insertData = insertPayload;
    OffsetNumber off = stack->off;

    // Check if there's enough space for insertion
    if (!entryIsEnoughSpace(btree, buf, off, insertData)) {
        // Split the page if not enough space
        entrySplitPage(btree, buf, stack, insertData, updateblkno,
                       newlpage, newrpage);
        return GPTP_SPLIT;
    }

    // Enough space - proceed with insertion
    return GPTP_INSERT;
}
```