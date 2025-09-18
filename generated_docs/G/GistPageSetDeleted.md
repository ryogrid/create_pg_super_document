# GistPageSetDeleted

## Location
src/include/access/gist.h: 204 - 214

## Overview
Marks a GiST index page as deleted and stores the transaction ID that performed the deletion for later recycling.

## Definition
```c
static inline void
GistPageSetDeleted(Page page, FullTransactionId deletexid)
```

## Detailed Description
This function marks a GiST (Generalized Search Tree) index page as deleted by setting the F_DELETED flag in the page's opaque area and storing the transaction ID that performed the deletion. The function ensures that the page is empty before marking it as deleted and modifies the page layout to accommodate the deletion metadata. The stored transaction ID is used later to determine when the page can be safely recycled.

The function modifies the page's pd_lower field to point to the end of the GISTDeletedPageContents structure, effectively changing the page layout from the normal tuple-based format to a simpler structure that only contains deletion metadata.

## Parameters / Member Variables
- `page`: The GiST index page to be marked as deleted
- `deletexid`: The full transaction ID of the transaction that is deleting this page

## Dependencies
- Functions called/Symbols referenced:
  - [PageIsEmpty](../P/PageIsEmpty.md)
  - GistPageGetOpaque
  - [PageGetContents](../P/PageGetContents.md)
  - MAXALIGN
  - SizeOfPageHeaderData
  - [GISTDeletedPageContents](GISTDeletedPageContents.md)
  - F_DELETED
  - PageHeader
- Called from (representative examples):
  - [gistdeletepage](../g/gistdeletepage.md)
  - [gistRedoPageDelete](../g/gistRedoPageDelete.md)

## Notes and Other Information
- The function includes an assertion that the page must be empty before deletion
- The page layout is modified so that pd_lower points to the end of the GISTDeletedPageContents structure
- The F_DELETED flag is set in the page's opaque area to mark it as deleted
- This is part of the GiST vacuum and page recycling mechanism
- The stored transaction ID allows the system to determine when it's safe to reuse the page