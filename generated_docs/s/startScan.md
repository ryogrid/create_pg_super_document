# startScan

## Location
[src/backend/access/gin/ginget.c:603-654](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginget.c#L603-L654)

## Overview
Initializes a GIN index scan by starting all scan entries and implementing fuzzy search optimizations to control result set size for performance.

## Definition

```c
static void
startScan(IndexScanDesc scan)
```
## Detailed Description
The startScan function orchestrates the initialization of a complete GIN index scan operation. It performs two main phases: first, it initializes all individual scan entries using startScanEntry, and second, it applies fuzzy search optimizations when GinFuzzySearchLimit is configured.

The fuzzy search optimization is a crucial performance feature that prevents queries from returning excessively large result sets. When all scan entries predict results larger than the fuzzy search threshold (totalentries * GinFuzzySearchLimit), the function reduces the predicted result counts by dividing them by the number of total entries. This heuristic approach trades some recall for significantly improved performance, particularly beneficial for queries involving very common terms.

After handling the fuzzy search logic and obtaining final entry frequency estimates, the function completes the scan initialization by calling startScanKey for each scan key, which partitions entries into required and additional sets for optimal scanning.

## Parameters / Member Variables
- `scan`: Index scan descriptor containing scan context, snapshot information, and opaque scan state
## Dependencies
- Functions called/Symbols referenced:
  - [startScanEntry](startScanEntry.md)
  - [startScanKey](startScanKey.md)
- Data types used:
  - [IndexScanDesc](../I/IndexScanDesc.md)
  - GinScanOpaque
  - [GinState](../G/GinState.md)
- Global variables:
  - GinFuzzySearchLimit (configuration parameter)
- Called from:
  - [gingetbitmap](../g/gingetbitmap.md)

## Notes and Other Information
- The fuzzy search reduction is a heuristic optimization that may affect query completeness but significantly improves performance for queries with very common terms
- The decision to reduce results is based on all entries exceeding the threshold, ensuring the optimization only applies when genuinely needed
- The reduction factor (dividing by totalentries) is a simple but effective way to scale down large result sets
- This function bridges the gap between entry-level initialization (startScanEntry) and key-level optimization (startScanKey)
- Essential for GIN bitmap scan operations, as called from gingetbitmap

## Simplified Source

```c
static void startScan(IndexScanDesc scan)
{
    GinScanOpaque so = (GinScanOpaque) scan->opaque;
    GinState *ginstate = &so->ginstate;
    uint32 i;

    // Initialize all scan entries
    for (i = 0; i < so->totalentries; i++)
        startScanEntry(ginstate, so->entries[i], scan->xs_snapshot);

    // Apply fuzzy search optimization if configured
    if (GinFuzzySearchLimit > 0) {
        bool reduce = true;

        // Check if all entries exceed the fuzzy search threshold
        for (i = 0; i < so->totalentries; i++) {
            if (so->entries[i]->predictNumberResult <= so->totalentries * GinFuzzySearchLimit) {
                reduce = false;
                break;
            }
        }

        // If all entries are too frequent, reduce their predicted results
        if (reduce) {
            for (i = 0; i < so->totalentries; i++) {
                so->entries[i]->predictNumberResult /= so->totalentries;
                so->entries[i]->reduceResult = true;
            }
        }
    }

    // Finish initializing scan keys with entry frequency estimates
    for (i = 0; i < so->nkeys; i++)
        startScanKey(ginstate, so, so->keys + i);
}
```