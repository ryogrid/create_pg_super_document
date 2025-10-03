# scanGetItem

## Location
[src/backend/access/gin/ginget.c:1287-1453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginget.c#L1287-L1453)

## Overview
Retrieves the next heap item pointer from a GIN index scan that matches all search keys using AND logic, advancing past a specified position.

## Definition

```c
static bool
scanGetItem(IndexScanDesc scan, ItemPointerData advancePast,
			ItemPointerData *item, bool *recheck)
```
## Detailed Description
This function implements the core logic for advancing through GIN index scan results by coordinating multiple key streams in lock-step fashion. It ensures that only heap item pointers that satisfy ALL search keys are returned, implementing AND logic for key combination. The function handles both exact and lossy page pointers, with special care taken to maintain correct ordering semantics. It continues scanning until either a matching item is found or all key streams are exhausted.

The function works by iterating through each scan key, fetching the next item that is greater than , and checking if all keys match the same item. If any key reports no match or is finished, the scan either advances or terminates. The logic is designed to work only when key streams don't mix exact and lossy pointers for the same page.

## Parameters / Member Variables
- : Index scan descriptor containing scan state and configuration
- : Item pointer position to advance beyond when searching  
- : Output parameter to store the next matching item pointer
- : Output parameter indicating if tuple needs rechecking with original conditions

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerSetMin
  - [keyGetItem](../k/keyGetItem.md)
  - ItemPointerIsLossyPage
  - GinItemPointerGetBlockNumber
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - [ginCompareItemPointers](../g/ginCompareItemPointers.md)
- Called from (representative examples):
  - [gingetbitmap](../g/gingetbitmap.md)

## Notes and Other Information
Critical for GIN bitmap scan performance as it coordinates multiple entry streams. The function assumes that key streams maintain proper ordering and don't contain conflicting exact/lossy pointers for the same page. The recheck flag is set when any key requires rechecking, which happens when lossy page references are involved or when the consistent function indicates uncertainty.

## Simplified Source

```c
static bool scanGetItem(IndexScanDesc scan, ItemPointerData advancePast,
                       ItemPointerData *item, bool *recheck)
{
    GinScanOpaque so = (GinScanOpaque) scan->opaque;
    uint32 i;
    bool match;

    // Advance keys in lock-step until finding an item that matches all keys
    do {
        CHECK_FOR_INTERRUPTS();

        ItemPointerSetMin(item);
        match = true;

        for (i = 0; i < so->nkeys && match; i++) {
            GinScanKey key = so->keys + i;

            // Skip excludeOnly keys for lossy pages
            if (ItemPointerIsLossyPage(item) && key->excludeOnly) {
                Assert(i > 0);
                continue;
            }

            // Get next item for this key > advancePast
            keyGetItem(&so->ginstate, so->tempCtx, key, advancePast);

            if (key->isFinished)
                return false;

            // If key doesn't match, advance past this item
            if (!key->curItemMatches) {
                advancePast = key->curItem;
                match = false;
                break;
            }

            // Key matches - update advancePast for other keys
            if (ItemPointerIsLossyPage(&key->curItem)) {
                if (GinItemPointerGetBlockNumber(&advancePast) <
                    GinItemPointerGetBlockNumber(&key->curItem)) {
                    ItemPointerSet(&advancePast,
                                  GinItemPointerGetBlockNumber(&key->curItem),
                                  InvalidOffsetNumber);
                }
            } else {
                Assert(GinItemPointerGetOffsetNumber(&key->curItem) > 0);
                ItemPointerSet(&advancePast,
                              GinItemPointerGetBlockNumber(&key->curItem),
                              OffsetNumberPrev(GinItemPointerGetOffsetNumber(&key->curItem)));
            }

            // Check if this matches the item from previous keys
            if (i == 0) {
                *item = key->curItem;
            } else {
                if (ItemPointerIsLossyPage(&key->curItem) ||
                    ItemPointerIsLossyPage(item)) {
                    Assert(GinItemPointerGetBlockNumber(&key->curItem) >=
                           GinItemPointerGetBlockNumber(item));
                    match = (GinItemPointerGetBlockNumber(&key->curItem) ==
                            GinItemPointerGetBlockNumber(item));
                } else {
                    Assert(ginCompareItemPointers(&key->curItem, item) >= 0);
                    match = (ginCompareItemPointers(&key->curItem, item) == 0);
                }
            }
        }
    } while (!match);

    Assert(!ItemPointerIsMin(item));

    // Set recheck flag if any key requires rechecking
    *recheck = false;
    for (i = 0; i < so->nkeys; i++) {
        GinScanKey key = so->keys + i;
        if (key->recheckCurItem) {
            *recheck = true;
            break;
        }
    }

    return true;
}
```