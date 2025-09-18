# scanGetItem

## Location
src/backend/access/gin/ginget.c: 1287 - 1453

## Overview
Retrieves the next heap item pointer from a GIN index scan that matches all search keys using AND logic, advancing past a specified position.

## Definition


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