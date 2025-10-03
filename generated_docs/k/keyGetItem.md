# keyGetItem

## Location
[src/backend/access/gin/ginget.c:992-1286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginget.c#L992-L1286)

## Overview
Identifies the current minimum item among entry streams for a GIN scan key, advances all streams past a specified position, and evaluates whether the current item satisfies the scan key's consistency conditions.

## Definition

```c
static void
keyGetItem(GinState *ginstate, MemoryContext tempCtx, GinScanKey key,
		   ItemPointerData advancePast)
```
## Detailed Description
The keyGetItem function implements the core logic for combining multiple GIN entry streams into a single scan key result. It operates in several phases: first finding the minimum item pointer among required entries, then advancing additional entries to the same position, and finally testing the combined result against the key's consistency function.

The function handles the complex interaction between exact and lossy page pointers, ensuring that lossy pointers (which indicate potential matches for all items on a heap page) are handled correctly. When lossy pointers are encountered, it uses a sophisticated strategy involving the tri-state consistency function to determine whether to return the lossy pointer or continue searching for exact matches.

The required/additional entry partitioning is crucial for performance: required entries must have matches for any valid result, while additional entries provide supplementary information. This allows the function to skip processing when required entries are exhausted while still using additional entries to refine results.

The consistency evaluation uses temporary memory contexts and supports both traditional boolean and tri-state consistency functions, enabling complex query logic including NOT operations and partial match scenarios.

## Parameters / Member Variables
- : Pointer to GIN state containing index metadata and configuration
- : Memory context for temporary allocations during consistency function calls
- : GIN scan key containing entry streams, consistency function, and result state
- : Item pointer indicating the minimum position for the next item to consider

## Dependencies
- Functions called/Symbols referenced:
  - [ginCompareItemPointers](../g/ginCompareItemPointers.md)
  - [entryGetItem](../e/entryGetItem.md)
  - ItemPointerSetMax
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - ItemPointerSetLossyPage
  - ItemPointerIsLossyPage
  - GinItemPointerGetBlockNumber
  - GinItemPointerGetOffsetNumber
  - OffsetNumberPrev
  - OffsetNumberNext
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
- Data types used:
  - [GinState](../G/GinState.md)
  - [GinScanKey](../G/GinScanKey.md)
  - [GinScanEntry](../G/GinScanEntry.md)
  - GinTernaryValue
  - [ItemPointerData](../I/ItemPointerData.md)
  - [MemoryContext](../M/MemoryContext.md)
- Constants:
  - GIN_TRUE, GIN_FALSE, GIN_MAYBE
  - InvalidOffsetNumber
- Called from:
  - [scanGetItem](../s/scanGetItem.md)

## Notes and Other Information
- Implements sophisticated lossy page pointer handling to avoid returning both exact and lossy pointers for the same page
- Uses tri-state logic (TRUE/FALSE/MAYBE) to handle complex consistency scenarios and partial information
- Critical performance optimization: processes required entries first and can short-circuit when they're exhausted
- The function maintains strict ItemPointer ordering requirements for higher-level scan coordination
- Handles exclude-only keys specially since they have no required entries by definition
- Memory management uses temporary contexts to ensure cleanup after consistency function calls
- The strategy for lossy pointers involves testing with MAYBE values to determine if whole-page matches are needed
- Enables sophisticated query optimization by allowing early termination and coordination with other scan keys

## Simplified Source

```c
static void keyGetItem(GinState *ginstate, MemoryContext tempCtx, GinScanKey key,
                      ItemPointerData advancePast)
{
    ItemPointerData minItem;
    ItemPointerData curPageLossy;
    uint32 i;
    bool haveLossyEntry;
    GinScanEntry entry;
    GinTernaryValue res;
    MemoryContext oldCtx;
    bool allFinished;

    Assert(!key->isFinished);

    // Early return if we already have a valid item > advancePast
    if (ginCompareItemPointers(&key->curItem, &advancePast) > 0)
        return;

    // Find minimum item > advancePast among required entries
    ItemPointerSetMax(&minItem);
    allFinished = true;
    for (i = 0; i < key->nrequired; i++) {
        entry = key->requiredEntries[i];

        if (entry->isFinished)
            continue;

        // Advance this entry if needed
        if (ginCompareItemPointers(&entry->curItem, &advancePast) <= 0) {
            entryGetItem(ginstate, entry, advancePast);
            if (entry->isFinished)
                continue;
        }

        allFinished = false;
        if (ginCompareItemPointers(&entry->curItem, &minItem) < 0)
            minItem = entry->curItem;
    }

    if (allFinished && !key->excludeOnly) {
        key->isFinished = true;
        return;
    }

    // Set advancePast based on minimum item found
    if (!key->excludeOnly) {
        if (ItemPointerIsLossyPage(&minItem)) {
            if (GinItemPointerGetBlockNumber(&advancePast) <
                GinItemPointerGetBlockNumber(&minItem)) {
                ItemPointerSet(&advancePast,
                              GinItemPointerGetBlockNumber(&minItem),
                              InvalidOffsetNumber);
            }
        } else {
            Assert(GinItemPointerGetOffsetNumber(&minItem) > 0);
            ItemPointerSet(&advancePast,
                          GinItemPointerGetBlockNumber(&minItem),
                          OffsetNumberPrev(GinItemPointerGetOffsetNumber(&minItem)));
        }
    } else {
        // Exclude-only: consider item just after advancePast
        Assert(key->nrequired == 0);
        ItemPointerSet(&minItem,
                      GinItemPointerGetBlockNumber(&advancePast),
                      OffsetNumberNext(GinItemPointerGetOffsetNumber(&advancePast)));
    }

    // Advance all additional entries
    for (i = 0; i < key->nadditional; i++) {
        entry = key->additionalEntries[i];

        if (entry->isFinished)
            continue;

        if (ginCompareItemPointers(&entry->curItem, &advancePast) <= 0) {
            entryGetItem(ginstate, entry, advancePast);
            if (entry->isFinished)
                continue;
        }

        // Update minItem if this additional entry has a smaller item
        if (ginCompareItemPointers(&entry->curItem, &minItem) < 0) {
            Assert(ItemPointerIsLossyPage(&minItem));
            minItem = entry->curItem;
        }
    }

    // Set up for consistency test
    key->curItem = minItem;
    ItemPointerSetLossyPage(&curPageLossy,
                           GinItemPointerGetBlockNumber(&key->curItem));
    haveLossyEntry = false;

    // Build entryRes array for consistency function
    for (i = 0; i < key->nentries; i++) {
        entry = key->scanEntry[i];
        if (entry->isFinished == false &&
            ginCompareItemPointers(&entry->curItem, &curPageLossy) == 0) {
            // Lossy entry on current page
            if (i < key->nuserentries)
                key->entryRes[i] = GIN_MAYBE;
            else
                key->entryRes[i] = GIN_TRUE;
            haveLossyEntry = true;
        } else
            key->entryRes[i] = GIN_FALSE;
    }

    oldCtx = MemoryContextSwitchTo(tempCtx);

    if (haveLossyEntry) {
        // Test if whole page matches with lossy entries
        res = key->triConsistentFn(key);
        if (res == GIN_TRUE || res == GIN_MAYBE) {
            MemoryContextSwitchTo(oldCtx);
            MemoryContextReset(tempCtx);
            key->curItem = curPageLossy;
            key->curItemMatches = true;
            key->recheckCurItem = true;
            return;
        }
    }

    // Test specific item with exact/lossy combination
    for (i = 0; i < key->nentries; i++) {
        entry = key->scanEntry[i];
        if (entry->isFinished)
            key->entryRes[i] = GIN_FALSE;
        else if (ginCompareItemPointers(&entry->curItem, &curPageLossy) == 0)
            key->entryRes[i] = GIN_MAYBE;
        else if (ginCompareItemPointers(&entry->curItem, &minItem) == 0)
            key->entryRes[i] = GIN_TRUE;
        else
            key->entryRes[i] = GIN_FALSE;
    }

    res = key->triConsistentFn(key);

    switch (res) {
        case GIN_TRUE:
            key->curItemMatches = true;
            break;
        case GIN_FALSE:
            key->curItemMatches = false;
            break;
        case GIN_MAYBE:
            key->curItemMatches = true;
            key->recheckCurItem = true;
            break;
        default:
            key->curItemMatches = true;
            key->recheckCurItem = true;
            break;
    }

    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(tempCtx);
}
```