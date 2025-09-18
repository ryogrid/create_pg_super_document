# _bt_slideleft

## Location
src/backend/access/nbtree/nbtsort.c: 683 - 713

## Overview
Slides the array of ItemIds on a page back one slot to remove the unneeded P_HIKEY line pointer space from rightmost pages during B-tree construction.

## Definition
```c
static void _bt_slideleft(Page rightmostpage)
```

## Detailed Description
This function performs a specialized optimization for rightmost pages in B-tree levels during index construction. Since _bt_blnewpage() always allocates space for a P_HIKEY line pointer, but rightmost pages don't actually need a high key, this function removes that unneeded space. It slides all ItemIds from P_FIRSTKEY onward back by one position, effectively overwriting the P_HIKEY slot and reclaiming the space. The operation also updates the page header's pd_lower field to reflect the reduced space usage.

## Parameters / Member Variables
- `rightmostpage`: Page pointer to the rightmost page that needs its P_HIKEY space reclaimed

## Dependencies
- Functions called/Symbols referenced:
  - ItemId (local variable type for line pointer manipulation)
  - PageGetMaxOffsetNumber (to get the highest valid offset on the page)
  - P_FIRSTKEY (constant for the first key position)
  - P_HIKEY (constant for the high key position being eliminated)
  - PageGetItemId (to access individual line pointers)
  - OffsetNumberNext (to iterate through offsets)
  - PageHeader (for page header access)
  - ItemIdData (for size calculation when adjusting pd_lower)
- Called from (representative examples):
  - _bt_uppershutdown

## Notes and Other Information
- This is a static function, only accessible within the nbtsort.c compilation unit
- Only called for rightmost pages since they don't need high keys for navigation
- Performs an in-place slide operation by copying ItemId structures sequentially
- Updates pd_lower to reflect the reclaimed space from removing one ItemIdData slot
- Part of the space optimization during B-tree construction that ensures efficient page layout
- The Assert ensures the page has at least one real data item (P_FIRSTKEY) before sliding
- Critical for maintaining proper page space accounting in the final index structure