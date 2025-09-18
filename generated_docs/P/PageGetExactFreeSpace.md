# PageGetExactFreeSpace

## Location
src/backend/storage/page/bufpage.c: 958 - 990

## Overview
Returns the size of the free (allocatable) space on a page, without any consideration for adding/removing line pointers.

## Definition


## Detailed Description
PageGetExactFreeSpace provides the raw calculation of free space available on a page by computing the difference between the upper and lower bounds of the page header (pd_upper and pd_lower). Unlike PageGetFreeSpace and PageGetFreeSpaceForMultipleTuples, this function does not account for line pointer overhead, making it useful for scenarios where the exact available space is needed without line pointer considerations.

The function uses signed arithmetic to handle edge cases where pd_lower might exceed pd_upper (indicating page corruption) and returns 0 in such cases. This function is commonly used by specialized access methods that manage their own line pointer allocation or when precise space calculations are required.

## Parameters / Member Variables
- : A pointer to the page for which to calculate exact free space

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (cast to access page header fields)
- Called from (representative examples):
  - [brin_can_do_samepage_update](../b/brin_can_do_samepage_update.md)
  - [writeListPage](../w/writeListPage.md)
  - [ginHeapTupleFastInsert](../g/ginHeapTupleFastInsert.md)
  - [_bt_dedup_pass](../b/_bt_dedup_pass.md)
  - [_bt_bottomupdel_pass](../b/_bt_bottomupdel_pass.md)
  - [_bt_findsplitloc](../b/_bt_findsplitloc.md)
  - [doPickSplit](../d/doPickSplit.md)
  - [spgAddNodeAction](../s/spgAddNodeAction.md)
  - [allocNewBuffer](../a/allocNewBuffer.md)
  - [SpGistGetBuffer](../S/SpGistGetBuffer.md)
  - [SpGistPageAddNewItem](../S/SpGistPageAddNewItem.md)
  - GinDataLeafPageGetFreeSpace
  - SpGistPageGetFreeSpace

## Notes and Other Information
- Does not account for line pointer overhead, unlike PageGetFreeSpace variants
- Used by specialized access methods that manage line pointers independently
- Useful for precise space calculations where line pointer overhead is handled separately
- Uses signed arithmetic to handle potential page corruption scenarios gracefully
- Returns 0 when pd_lower exceeds pd_upper (corrupted page scenario)
- Located in src/backend/storage/page/bufpage.c:958-990