# startScanKey

## Location
src/backend/access/gin/ginget.c: 505 - 602

## Overview
Initializes a GIN scan key by dividing its entries into required and additional sets to optimize scanning performance, particularly for complex queries with both frequent and rare terms.

## Definition


## Detailed Description
The startScanKey function prepares a GIN (Generalized Inverted Index) scan key for efficient scanning by intelligently partitioning scan entries into two categories: required and additional. This optimization is crucial for complex queries involving multiple terms with varying frequencies.

The function implements a sophisticated algorithm that sorts entries by frequency (using predictNumberResult) and determines the minimal set of required entries needed for a match. Frequent terms are preferentially placed in the additional set, allowing the scanner to skip over items that only match additional entries without corresponding matches in required entries. This dramatically improves performance for queries like "frequent & rare" where the frequent term can be treated as additional.

For exclude-only scan keys, all entries are placed in the additional set since no positive matches are required. For single-entry keys, the lone entry becomes required by default.

## Parameters / Member Variables
- : Pointer to GIN state information containing index metadata
- : GIN scan opaque structure containing scan context and memory contexts
- : The GIN scan key to be initialized and partitioned

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