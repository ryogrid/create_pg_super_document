# btrestrpos

## Location
[src/backend/access/nbtree/nbtree.c:479-536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L479-L536)

## Overview
Restores a B-tree index scan to the position previously saved by btmarkpos, handling both simple within-page and complex cross-page restoration scenarios.

## Definition

```c
void
btrestrpos(IndexScanDesc scan)
```
## Detailed Description
The btrestrpos function restores a B-tree index scan to a previously marked position. It implements two different restoration strategies depending on whether the scan has moved to a different page since the mark was set. For simple cases where the scan remained on the same page, it only restores the item index. For complex cases where the scan moved to a different page, it performs a full position restoration including buffer handling, killed items processing, and tuple workspace copying. The function also handles array key reinitialization when necessary.

## Parameters / Member Variables
- `scan`: The IndexScanDesc structure representing the scan to be restored to its marked position
## Dependencies
- Functions called/Symbols referenced:
  - BTScanPosIsValid
  - [_bt_killitems](_bt_killitems.md)
  - BTScanPosUnpinIfPinned
  - BTScanPosIsPinned
  - [IncrBufferRefCount](../I/IncrBufferRefCount.md)
  - [_bt_start_array_keys](_bt_start_array_keys.md)
  - BTScanPosInvalidate
  - [IndexScanDesc](../I/IndexScanDesc.md)
  - BTScanOpaque
  - [BTScanPosData](../B/BTScanPosData.md)
  - [BTScanPosItem](../B/BTScanPosItem.md)
- Called from (representative examples):
  - [bthandler](bthandler.md)

## Notes and Other Information
- Uses markItemIndex >= 0 as an indicator that the scan hasn't moved to a new page since marking
- Performs full buffer management including reference count increments and killed items processing for cross-page restoration
- Copies both position data structure and tuple workspace (currTuples) when restoring from a different page
- Reinitializes array keys using _bt_start_array_keys when scan involves array operations
- The function complements the lazy marking approach used in btmarkpos by handling the complex restoration logic only when needed

## Simplified Source

```c
void btrestrpos(IndexScanDesc scan) {
    BTScanOpaque so = (BTScanOpaque) scan->opaque;

    if (so->markItemIndex >= 0) {
        // Simple case: still on same page since mark
        so->currPos.itemIndex = so->markItemIndex;
    } else {
        // Complex case: scan moved to different page since mark

        // Clean up current position
        if (BTScanPosIsValid(so->currPos)) {
            // Process any killed items before leaving page
            if (so->numKilled > 0)
                _bt_killitems(scan);
            BTScanPosUnpinIfPinned(so->currPos);
        }

        // Restore to marked position if valid
        if (BTScanPosIsValid(so->markPos)) {
            // Increment buffer reference count
            if (BTScanPosIsPinned(so->markPos))
                IncrBufferRefCount(so->markPos.buf);

            // Copy position data structure
            memcpy(&so->currPos, &so->markPos,
                   offsetof(BTScanPosData, items[1]) +
                   so->markPos.lastItem * sizeof(BTScanPosItem));

            // Copy tuple workspace if exists
            if (so->currTuples)
                memcpy(so->currTuples, so->markTuples,
                       so->markPos.nextTupleOffset);

            // Reinitialize array keys if needed
            if (so->numArrayKeys) {
                _bt_start_array_keys(scan, so->currPos.dir);
                so->needPrimScan = false;
            }
        } else {
            BTScanPosInvalidate(so->currPos);
        }
    }
}
```