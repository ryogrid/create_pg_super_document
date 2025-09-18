# PageIndexMultiDelete

## Location
src/backend/storage/page/bufpage.c: 1161 - 1294

## Overview
Efficiently deletes multiple tuples from an index page simultaneously, significantly faster than multiple individual deletions when processing more than 2 items.

## Definition


## Detailed Description
PageIndexMultiDelete is an optimized function for removing multiple tuples from an index page at once. It performs bulk deletion by rebuilding the line pointer array without the deleted items and then compacting the remaining tuple data. The function includes extensive validation checks and uses different strategies based on the number of items to delete:

- For 2 or fewer items: delegates to individual PageIndexTupleDelete calls in reverse order
- For more items: performs bulk processing by scanning line pointers, building a new array excluding deleted items, and compacting the remaining data

The function requires that the item numbers array be provided in sorted order and performs comprehensive corruption checks on page structure before making any modifications.

## Parameters / Member Variables
- : The index page from which to delete tuples
- : Array of item offset numbers to delete, must be in sorted order
- : Number of items in the itemnos array, must be ≤ MaxIndexTuplesPerPage

## Dependencies
- Functions called/Symbols referenced:
  - [PageIndexTupleDelete](PageIndexTupleDelete.md)
  - [PageGetMaxOffsetNumber](PageGetMaxOffsetNumber.md)
  - [PageGetItemId](PageGetItemId.md)
  - ItemIdHasStorage
  - ItemIdGetLength
  - ItemIdGetOffset
  - [compactify_tuples](../c/compactify_tuples.md)
- Called from (representative examples):
  - [_bt_delitems_vacuum](../b/_bt_delitems_vacuum.md) (B-tree vacuum operations)
  - [_hash_vacuum_one_page](../h/_hash_vacuum_one_page.md) (Hash index vacuum)
  - [gistprunepage](../g/gistprunepage.md) (GiST index page pruning)
  - [spgPageIndexMultiDelete](../s/spgPageIndexMultiDelete.md) (SP-GiST operations)

## Notes and Other Information
- Critical requirement: item numbers must be provided in ascending order
- Includes magic number threshold (currently 2) below which individual deletions are preferred
- Performs extensive page corruption validation before modification
- Uses temporary arrays (itemidbase, newitemids) to build new page structure before committing changes
- Optimizes for presorted data during tuple compaction
- Essential for efficient bulk deletion operations in various index types (B-tree, Hash, GiST, SP-GiST)