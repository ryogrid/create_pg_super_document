# spgPageIndexMultiDelete

## Location
src/backend/access/spgist/spgdoinsert.c: 131 - 185

## Overview
Efficiently deletes multiple tuples from an SP-GiST index page while preserving tuple offset numbers by replacing them with dead tuples of specified types.

## Definition


## Detailed Description
This function performs a bulk deletion operation on an SP-GiST index page by replacing multiple tuples with dead tuples rather than physically removing them. This approach preserves the tuple offset numbering scheme which is crucial for SP-GiST index consistency. The function first sorts the offset numbers for efficiency, uses PageIndexMultiDelete to remove the original tuples, then inserts appropriately typed dead tuples (REDIRECT, DEAD, or PLACEHOLDER) in their place. The first tuple in the list gets the 'firststate' type while remaining tuples get the 'reststate' type. This function is designed to work safely during WAL replay and within critical sections.

## Parameters / Member Variables
- `state`: SP-GiST state information containing type-specific configuration
- `page`: The index page from which tuples will be deleted
- `itemnos`: Array of offset numbers identifying tuples to be deleted
- `nitems`: Number of items in the itemnos array
- `firststate`: Dead tuple type for the first item (REDIRECT/DEAD/PLACEHOLDER)
- `reststate`: Dead tuple type for remaining items (REDIRECT/DEAD/PLACEHOLDER)
- `blkno`: Block number for redirection (used when state is REDIRECT)
- `offnum`: Offset number for redirection (used when state is REDIRECT)

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (copies offset numbers array)
  - qsort (sorts offset numbers using cmpOffsetNumbers)
  - [cmpOffsetNumbers](../c/cmpOffsetNumbers.md) (comparator for sorting offset numbers)
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md) (removes original tuples from page)
  - [spgFormDeadTuple](spgFormDeadTuple.md) (creates dead tuple structures)
  - PageAddItem (adds dead tuples back to page)
  - SpGistPageGetOpaque (accesses page metadata)
  - elog (error reporting)
- Called from (representative examples):
  - [moveLeafs](../m/moveLeafs.md)
  - [doPickSplit](../d/doPickSplit.md)
  - [vacuumLeafPage](../v/vacuumLeafPage.md)
  - [spgRedoMoveLeafs](spgRedoMoveLeafs.md)
  - [spgRedoPickSplit](spgRedoPickSplit.md)
  - [spgRedoVacuumLeaf](spgRedoVacuumLeaf.md)

## Notes and Other Information
- Designed to be safe for use during WAL replay and in critical sections (no palloc calls)
- Sorts offset numbers internally for efficiency but preserves the caller's original array
- Updates page metadata (nRedirection, nPlaceholder counters) based on dead tuple types
- The function handles the case where nitems is 0 by returning early
- Dead tuple reuse optimization: reuses the same dead tuple structure when consecutive items have the same state
- Located in src/backend/access/spgist/spgdoinsert.c:131-185