# _bt_singleval_fillfactor

## Location
src/backend/access/nbtree/nbtdedup.c: 822 - 863

## Overview
Reduces the maximum posting list size when using the "single value" strategy to ensure proper page fill factor coordination with the page splitting logic.

## Definition
```c
static void _bt_singleval_fillfactor(Page page, BTDedupState state, Size newitemsz)
```

## Detailed Description
This function implements a critical component of the "single value" deduplication strategy by dynamically adjusting the maximum posting list size (maxpostingsize) to prevent the creation of a problematic sixth posting list tuple that would interfere with optimal page splitting.

The function performs a calculation that must match the logic in nbtsplitloc.c to ensure consistency between deduplication and page splitting behaviors. It calculates the target free space that should remain on the left half of a page after splitting, then reduces the maximum posting list size by an equivalent amount.

The goal is to ensure that when a page containing only duplicates of a single value eventually splits, it ends up BTREE_SINGLEVAL_FILLFACTOR% full, just as it would if deduplication were disabled. This prevents the creation of a sixth posting list tuple that would be smaller than the first five, maintaining uniformity and predictable behavior.

The calculation works by:
1. Computing the available space on a page (total page size minus headers and opaque data)
2. Subtracting space needed for the new high key (including pivot heap TID space)
3. Calculating the target reduction based on the desired fill factor percentage
4. Reducing maxpostingsize by this amount (or setting it to 0 if the reduction exceeds the current value)

## Parameters / Member Variables
- `page`: The B-tree page being processed for deduplication
- `state`: The deduplication state whose maxpostingsize will be modified
- `newitemsz`: The size of the new item being inserted, used in free space calculations

## Dependencies
- Functions called/Symbols referenced:
  - PageGetPageSize
  - SizeOfPageHeaderData (constant)
  - BTPageOpaqueData (type)
  - BTREE_SINGLEVAL_FILLFACTOR (constant)
- Called from:
  - _bt_dedup_pass

## Notes and Other Information
- This is a static function within the nbtdedup.c module, part of PostgreSQL's B-tree deduplication system
- The calculation deliberately matches logic in nbtsplitloc.c to ensure consistent behavior between deduplication and page splitting
- Prevents creation of asymmetric posting list tuples that could interfere with split point selection
- The function may set maxpostingsize to 0 if the required reduction exceeds the current value
- Works as part of a multi-pass deduplication strategy where early passes handle different tuple types
- Located at src/backend/access/nbtree/nbtdedup.c:822-863