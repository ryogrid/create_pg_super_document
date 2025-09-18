# _bt_delete_or_dedup_one_page

## Location
src/backend/access/nbtree/nbtinsert.c: 2683 - 2811

## Overview
Attempts to avoid a leaf page split by performing deletion of dead tuples, bottom-up deletion, and deduplication operations to free space for a new item insertion.

## Definition
```c
static void _bt_delete_or_dedup_one_page(Relation rel, Relation heapRel, BTInsertState insertstate, bool simpleonly, bool checkingunique, bool uniquedup, bool indexUnchanged)
```

## Detailed Description
The `_bt_delete_or_dedup_one_page` function implements a multi-stage approach to avoid expensive page splits during B-tree insertions. It performs up to three different space reclamation strategies in order of increasing complexity:

1. **Simple deletion**: Removes tuples marked with LP_DEAD flags
2. **Bottom-up deletion**: Removes index tuples for heap tuples that were recently deleted
3. **Deduplication**: Merges multiple index tuples with identical key values

The function employs a cascading strategy where simpler operations are attempted first, and more complex operations are only considered if simpler ones fail to create sufficient space. This approach balances performance with space efficiency.

The function maintains several optimization hints and conditions to determine which strategies are appropriate for the current insertion context, particularly considering unique constraints and executor hints about data changes.

## Parameters / Member Variables
- `rel`: The index relation being modified
- `heapRel`: The corresponding heap relation
- `insertstate`: Current insertion state containing buffer, item size, and other context
- `simpleonly`: If true, only perform simple deletion without fallback strategies
- `checkingunique`: Indicates this is part of a unique constraint check
- `uniquedup`: Hint that duplicate values may exist for unique constraints  
- `indexUnchanged`: Executor hint that the logical content hasn't changed

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_simpledel_pass](_bt_simpledel_pass.md): Performs simple deletion of dead tuples
  - [_bt_bottomupdel_pass](_bt_bottomupdel_pass.md): Performs bottom-up deletion of obsolete index entries
  - [_bt_dedup_pass](_bt_dedup_pass.md): Performs deduplication of identical key values
  - [PageGetFreeSpace](../P/PageGetFreeSpace.md): Checks available space after operations
  - `BTGetDeduplicateItems`: Checks if deduplication is enabled for the index
- Called from (representative examples):
  - [_bt_findinsertloc](_bt_findinsertloc.md): During insertion location finding when space is tight

## Notes and Other Information
- Only operates on leaf pages (assertion enforces this)
- Simple deletion scans all items regardless of BTP_HAS_GARBAGE flag for better coverage
- Bottom-up deletion requires heapkeyspace indexes for correctness
- Deduplication requires the index to support equal image representation (allequalimage)
- Function may invalidate cached page bounds due to tuple removal
- BTP_HAS_GARBAGE flag cleanup is deferred to avoid unnecessary writes
- Returns early when sufficient space is freed to avoid unnecessary work
- Balances scan overhead against split avoidance benefits