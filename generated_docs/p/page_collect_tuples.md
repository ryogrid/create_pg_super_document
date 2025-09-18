# page_collect_tuples

## Location
src/backend/access/heap/heapam.c: 488 - 537

## Overview
page_collect_tuples is an inline helper function that scans through all tuples on a heap page and collects visible tuple offsets, serving as the core tuple visibility filtering loop for heap page scanning operations.

## Definition


## Detailed Description
This function iterates through all item identifiers on a heap page and evaluates each tuple's visibility according to the provided snapshot. It's designed as an always-inline function to maximize performance during sequential scans. The function handles both optimized paths (when all tuples are known to be visible) and general paths (requiring full visibility checks). For each visible tuple, it records the offset number in the scan descriptor's rs_vistuples array, enabling efficient tuple retrieval in subsequent operations.

## Parameters / Member Variables
- : HeapScanDesc containing the scan state and result storage
- : Snapshot defining transaction visibility rules
- : The heap page being scanned
- : Buffer containing the page (needed for visibility checks)
- : Block number of the page being scanned
- : Number of line pointers (item identifiers) on the page
- : Optimization flag indicating all tuples are known to be visible
- : Whether to perform serializable conflict detection

## Dependencies
- Functions called/Symbols referenced:
  - PageGetItemId
  - ItemIdIsNormal
  - PageGetItem
  - ItemIdGetLength
  - RelationGetRelid
  - ItemPointerSet
  - HeapTupleSatisfiesVisibility
  - HeapCheckForSerializableConflictOut
- Called from (representative examples):
  - heap_prepare_pagescan (multiple call sites)

## Notes and Other Information
- Marked as pg_attribute_always_inline for performance optimization
- Returns the number of visible tuples found (ntup)
- Uses MaxHeapTuplesPerPage assertion to ensure array bounds safety
- Optimizes visibility checking when all_visible is true
- Handles serializable isolation level conflict detection when required
- Core building block for heap scanning operations in PostgreSQL