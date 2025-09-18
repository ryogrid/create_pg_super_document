# checkSplitConditions

## Location
src/backend/access/spgist/spgdoinsert.c: 333 - 386

## Overview
This function counts the number and total size of leaf tuples in a chain to help determine if a page split is needed in SPGiST index operations.

## Definition


## Detailed Description
The function traverses a chain of leaf tuples starting at current->offnum to count live tuples and calculate their total storage size. It handles a special case for root pages where it returns artificially large values to force spgdoinsert() to use the doPickSplit code path instead of moveLeafs, since moveLeafs cannot handle root pages.

The function walks through the linked chain of tuples using SGLT_GET_NEXTOFFSET(), counting only SPGIST_LIVE tuples while ignoring SPGIST_DEAD tuples (which won't be moved during splits). For each live tuple, it adds the tuple size plus the ItemIdData overhead to get the true storage cost.

## Parameters / Member Variables
- : The SPGiST index relation being examined
- : SPGiST state information (not actively used in this function)
- : Page descriptor containing the starting offset for the chain to examine
- : Output parameter that receives the count of live tuples in the chain

## Dependencies
- Functions called/Symbols referenced:
  - SpGistBlockIsRoot (checks if the current block is the root page)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md) (gets maximum valid offset number on page)
  - [PageGetItem](../P/PageGetItem.md)/PageGetItemId (page item access functions)
  - SGLT_GET_NEXTOFFSET (macro to get next offset in leaf tuple chain)
- Called from (representative examples):
  - [spgdoinsert](../s/spgdoinsert.md) (at src/backend/access/spgist/spgdoinsert.c:2123)

## Notes and Other Information
- Returns the total size in bytes needed to store all live tuples in the chain
- Special handling for root pages: returns BLCKSZ for both count and size to force splitting
- Only counts SPGIST_LIVE tuples; SPGIST_DEAD tuples are ignored since they won't be moved
- Includes ItemIdData overhead in size calculations for accurate space requirements
- DEAD tuples are expected only as the first item in a chain and must have InvalidOffsetNumber as next
- Uses assertions to validate tuple state consistency and offset bounds
- Location: src/backend/access/spgist/spgdoinsert.c:333-386