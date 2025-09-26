# _bt_sortaddtup

## Location
src/backend/access/nbtree/nbtsort.c: 714 - 783

## Overview
A static function that adds an item to a page being built during B-tree index construction, with special handling for first data items and proper error checking.

## Definition


## Detailed Description
This function is very similar to nbtinsert.c's , but this variant raises an error directly rather than returning a status code. It is specifically designed for use during B-tree index sorting and building operations.

The function handles a special case for the first data item on a page. When  is true, it creates a truncated tuple containing only the IndexTupleData header with no attributes, which serves as a placeholder. This optimization is related to B-tree page layout conventions where the key portion of the first item on non-leaf pages need not be stored.

The caller does not know yet if the page will be rightmost, so offset P_FIRSTKEY is always assumed to be the first data key. Pages that turn out to be rightmost on their level are fixed later by calling .

## Parameters / Member Variables
- : The page being built to which the item will be added
- : Size of the item to be added
- : The IndexTuple to be added to the page
- : The offset number where the item should be placed
- : Boolean flag indicating if this is the first data item, requiring special truncation handling

## Dependencies
- Functions called/Symbols referenced:
  - IndexTupleData
  - BTreeTupleSetNAtts
  - PageAddItem
  - Item
  - InvalidOffsetNumber
  - elog
- Called from (representative examples):
  - _bt_buildadd

## Notes and Other Information
- This is a static function used internally within nbtsort.c during index construction
- The function will raise an ERROR if PageAddItem fails, ensuring that index building failures are caught immediately
- The truncation logic for newfirstdataitem creates a minimal tuple with zero attributes, optimizing space usage for internal pages
- Part of PostgreSQL's B-tree index building infrastructure, specifically the sorting phase