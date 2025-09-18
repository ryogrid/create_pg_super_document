# BTScanPosItem

## Location
src/include/access/nbtree.h: 944 - 949

## Overview
BTScanPosItem is a structure that stores information about each matching item found during a B-tree index scan, including heap TID, index offset, and tuple workspace location.

## Definition


## Detailed Description
This structure represents what the B-tree access method remembers about each matching item during an index scan. It is part of the page-at-a-time scanning approach where the system pins and read-locks a page, identifies all matching items, saves them in BTScanPosItem structures, then releases the read-lock while returning items to the caller. This minimizes lock/unlock traffic while maintaining necessary synchronization for VACUUM operations.

## Parameters / Member Variables
- : ItemPointerData containing the TID (tuple identifier) of the referenced heap item
- : OffsetNumber specifying the index item's location within the current page
- : LocationIndex indicating the IndexTuple's offset in the workspace array (used for index-only scans)

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerData
  - OffsetNumber
  - LocationIndex
- Called from (representative examples):
  - btrestrpos
  - _bt_first
  - _bt_next
  - _bt_saveitem
  - _bt_setuppostingitems
  - _bt_savepostingitem
  - _bt_steppage
  - _bt_endpoint
  - _bt_killitems
  - BTScanPosData

## Notes and Other Information
- Used in both regular index scans and index-only scans
- For index-only scans, the entire IndexTuple is saved in a separate workspace array
- For posting list tuples, a base tuple is stored once and shared across multiple TIDs
- Part of the page-at-a-time scanning strategy that optimizes lock usage
- Essential for VACUUM synchronization mechanisms in B-tree operations