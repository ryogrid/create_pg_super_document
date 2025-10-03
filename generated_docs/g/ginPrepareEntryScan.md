# ginPrepareEntryScan

## Location
[src/backend/access/gin/ginentrypage.c:747-774](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L747-L774)

## Overview
Initializes a GinBtree structure for entry page access by setting up all necessary function pointers and parameters for GIN entry tree operations.

## Definition
void ginPrepareEntryScan(GinBtree btree, OffsetNumber attnum, Datum key, GinNullCategory category, GinState *ginstate)

## Detailed Description
This function sets up a GinBtree structure specifically configured for operations on GIN entry pages (as opposed to posting pages). It initializes the btree structure with appropriate function pointers for entry-specific operations and configures parameters for scanning or modifying entry trees. The function serves as a factory method that creates a properly configured btree object for entry page operations.

The function assigns specialized entry page functions to handle:
- Page navigation (findChildPage, getLeftMostChild, isMoveRight)
- Item location and manipulation (findItem, findChildPtr)
- Page modification operations (beginPlaceToPage, execPlaceToPage, fillRoot, prepareDownlink)
- Tree structure management

The function also handles WAL recovery scenarios where the ginstate may contain minimal valid data.

## Parameters / Member Variables
- btree: GinBtree structure to be initialized for entry page operations
- attnum: Attribute number for the indexed column
- key: The key value being searched for or inserted
- category: NULL category classification for the key (NULL, NOT_NULL, etc.)
- ginstate: GIN access method state containing index metadata and configuration

## Dependencies
- Functions called/Symbols referenced:
  - memset
  - [entryLocateEntry](../e/entryLocateEntry.md)
  - [entryGetLeftMostPage](../e/entryGetLeftMostPage.md)
  - [entryIsMoveRight](../e/entryIsMoveRight.md)
  - [entryLocateLeafEntry](../e/entryLocateLeafEntry.md)
  - [entryFindChildPtr](../e/entryFindChildPtr.md)
  - [entryBeginPlaceToPage](../e/entryBeginPlaceToPage.md)
  - [entryExecPlaceToPage](../e/entryExecPlaceToPage.md)
  - [ginEntryFillRoot](ginEntryFillRoot.md)
  - [entryPrepareDownlink](../e/entryPrepareDownlink.md)
  - GIN_ROOT_BLKNO
- Called from (representative examples):
  - [startScanEntry](../s/startScanEntry.md)
  - [ginEntryInsert](ginEntryInsert.md)
  - [GinBtreeDataLeafInsertData](../G/GinBtreeDataLeafInsertData.md) (via function pointer setup)

## Notes and Other Information
- The function sets isData=false to indicate this btree is for entry pages, not posting pages
- During WAL recovery, the ginstate may only contain a faked-up Relation pointer with bogus key data
- The fullScan and isBuild flags are set to false, indicating this is for targeted operations rather than bulk operations
- All entry-specific function pointers are assigned to handle the specialized behavior of entry tree operations
- The function uses memset to zero-initialize the entire btree structure before setting specific fields

## Simplified Source

```c
// Simplified version of ginPrepareEntryScan
void ginPrepareEntryScan(GinBtree btree, OffsetNumber attnum,
                         Datum key, GinNullCategory category,
                         GinState *ginstate)
{
    // Initialize structure to zero
    memset(btree, 0, sizeof(GinBtreeData));

    // Set basic properties
    btree->index = ginstate->index;
    btree->rootBlkno = GIN_ROOT_BLKNO;
    btree->ginstate = ginstate;

    // Configure entry-specific function pointers
    btree->findChildPage = entryLocateEntry;
    btree->getLeftMostChild = entryGetLeftMostPage;
    btree->isMoveRight = entryIsMoveRight;
    btree->findItem = entryLocateLeafEntry;
    btree->findChildPtr = entryFindChildPtr;
    btree->beginPlaceToPage = entryBeginPlaceToPage;
    btree->execPlaceToPage = entryExecPlaceToPage;
    btree->fillRoot = ginEntryFillRoot;
    btree->prepareDownlink = entryPrepareDownlink;

    // Set operation flags
    btree->isData = false;
    btree->fullScan = false;
    btree->isBuild = false;

    // Set search parameters
    btree->entryAttnum = attnum;
    btree->entryKey = key;
    btree->entryCategory = category;
}
```