# addLeafTuple

## Location
src/backend/access/spgist/spgdoinsert.c: 203 - 332

## Overview
This function adds a leaf tuple to a leaf page in an SPGiST (Space-Partitioned GiST) index where there is known to be room for it, handling both new chains and existing chains of tuples.

## Definition


## Detailed Description
The function manages the insertion of leaf tuples into SPGiST index pages, handling two main scenarios:
1. **New chain creation**: When the tuple is not part of an existing chain (current->offnum is invalid or the block is root)
2. **Existing chain insertion**: When the tuple must be inserted into an existing chain, either as a second element or replacing a DEAD tuple

The function operates within a critical section and handles WAL (Write-Ahead Logging) for crash recovery. It maintains proper chain linkage by setting next offset pointers and updates parent downlinks when necessary.

## Parameters / Member Variables
- : The SPGiST index relation being modified
- : SPGiST state information containing configuration and temporary data
- : The leaf tuple to be inserted into the page
- : Page descriptor for the current leaf page where tuple will be inserted
- : Page descriptor for the parent page (may be InvalidBuffer if no parent)
- : Boolean indicating if this is a nulls page
- : Boolean indicating if this is a newly allocated page

## Dependencies
- Functions called/Symbols referenced:
  - SGLT_SET_NEXTOFFSET (macro for setting next offset in leaf tuple)
  - [SpGistPageAddNewItem](../S/SpGistPageAddNewItem.md) (adds new item to SPGiST page)
  - SpGistBlockIsRoot (checks if block is root)
  - [saveNodeLink](../s/saveNodeLink.md) (updates parent's downlink)
  - [PageGetItem](../P/PageGetItem.md)/PageGetItemId (page item access functions)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md)/PageAddItem (page modification functions)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)/XLogInsert (WAL logging functions)
- Called from (representative examples):
  - [spgdoinsert](../s/spgdoinsert.md) (at src/backend/access/spgist/spgdoinsert.c:2118)

## Notes and Other Information
- Operates within START_CRIT_SECTION() and END_CRIT_SECTION() for atomicity
- Handles three tuple states: SPGIST_LIVE, SPGIST_DEAD, and error cases
- For existing chains with LIVE head tuples, inserts the new tuple as the second element to avoid changing the chain head address
- For DEAD head tuples, replaces the dead tuple in-place
- Supports WAL logging when RelationNeedsWAL() returns true and not during index build
- Uses spgxlogAddLeaf structure for WAL record information
- Location: src/backend/access/spgist/spgdoinsert.c:203-332