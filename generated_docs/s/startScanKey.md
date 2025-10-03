# startScanKey

## Location
[src/backend/access/gin/ginget.c:505-602](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginget.c#L505-L602)

## Overview
Initializes a GIN scan key by dividing its entries into required and additional sets to optimize scanning performance, particularly for complex queries with both frequent and rare terms.

## Definition

```c
static void
startScanKey(GinState *ginstate, GinScanOpaque so, GinScanKey key)
```
## Detailed Description
The startScanKey function prepares a GIN (Generalized Inverted Index) scan key for efficient scanning by intelligently partitioning scan entries into two categories: required and additional. This optimization is crucial for complex queries involving multiple terms with varying frequencies.

The function implements a sophisticated algorithm that sorts entries by frequency (using predictNumberResult) and determines the minimal set of required entries needed for a match. Frequent terms are preferentially placed in the additional set, allowing the scanner to skip over items that only match additional entries without corresponding matches in required entries. This dramatically improves performance for queries like "frequent & rare" where the frequent term can be treated as additional.

For exclude-only scan keys, all entries are placed in the additional set since no positive matches are required. For single-entry keys, the lone entry becomes required by default.

## Parameters / Member Variables
- `*ginstate`: Pointer to GIN state information containing index metadata
- `so`: GIN scan opaque structure containing scan context and memory contexts
- `key`: The GIN scan key to be initialized and partitioned
## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerSetMin
  - [entryIndexByFrequencyCmp](../e/entryIndexByFrequencyCmp.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - CHECK_FOR_INTERRUPTS
- Data types used:
  - [GinState](../G/GinState.md)
  - GinScanOpaque
  - [GinScanKey](../G/GinScanKey.md)
  - [GinScanEntry](../G/GinScanEntry.md)
  - GIN_FALSE, GIN_MAYBE (enum values)
- Called from:
  - [startScan](startScan.md)

## Notes and Other Information
- Uses multiple memory contexts (keyCtx, tempCtx) for proper memory management during scan initialization
- The partitioning algorithm calls the triConsistentFn to determine the minimum required set
- Implements an interruptible loop to handle cases with many scan keys
- Critical for GIN index performance optimization, especially for complex boolean queries
- The required/additional partitioning directly impacts scan efficiency by enabling selective item skipping

## Simplified Source

```c
static void startScanKey(GinState *ginstate, GinScanOpaque so, GinScanKey key)
{
    MemoryContext oldCtx = CurrentMemoryContext;
    int i, j;
    int *entryIndexes;

    // Initialize scan key state
    ItemPointerSetMin(&key->curItem);
    key->curItemMatches = false;
    key->recheckCurItem = false;
    key->isFinished = false;

    if (key->excludeOnly) {
        // Exclude-only keys: all entries are additional
        MemoryContextSwitchTo(so->keyCtx);
        key->nrequired = 0;
        key->nadditional = key->nentries;
        key->additionalEntries = palloc(key->nadditional * sizeof(GinScanEntry));
        for (i = 0; i < key->nadditional; i++)
            key->additionalEntries[i] = key->scanEntry[i];
    } else if (key->nentries > 1) {
        // Multiple entries: partition into required and additional sets
        MemoryContextSwitchTo(so->tempCtx);

        // Sort entries by frequency (least frequent first)
        entryIndexes = (int *) palloc(sizeof(int) * key->nentries);
        for (i = 0; i < key->nentries; i++)
            entryIndexes[i] = i;
        qsort_arg(entryIndexes, key->nentries, sizeof(int),
                  entryIndexByFrequencyCmp, key);

        // Find minimum required set using triConsistentFn
        for (i = 1; i < key->nentries; i++)
            key->entryRes[entryIndexes[i]] = GIN_MAYBE;

        for (i = 0; i < key->nentries - 1; i++) {
            key->entryRes[entryIndexes[i]] = GIN_FALSE;
            if (key->triConsistentFn(key) == GIN_FALSE)
                break;
            CHECK_FOR_INTERRUPTS();
        }

        // Set up required and additional entry arrays
        MemoryContextSwitchTo(so->keyCtx);
        key->nrequired = i + 1;
        key->nadditional = key->nentries - key->nrequired;
        key->requiredEntries = palloc(key->nrequired * sizeof(GinScanEntry));
        key->additionalEntries = palloc(key->nadditional * sizeof(GinScanEntry));

        j = 0;
        for (i = 0; i < key->nrequired; i++)
            key->requiredEntries[i] = key->scanEntry[entryIndexes[j++]];
        for (i = 0; i < key->nadditional; i++)
            key->additionalEntries[i] = key->scanEntry[entryIndexes[j++]];

        MemoryContextReset(so->tempCtx);
    } else {
        // Single entry: it's required
        MemoryContextSwitchTo(so->keyCtx);
        key->nrequired = 1;
        key->nadditional = 0;
        key->requiredEntries = palloc(1 * sizeof(GinScanEntry));
        key->requiredEntries[0] = key->scanEntry[0];
    }

    MemoryContextSwitchTo(oldCtx);
}
```