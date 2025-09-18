# btendscan

## Location
src/backend/access/nbtree/nbtree.c: 417 - 452

## Overview
Terminates a B-tree index scan by cleaning up resources, handling killed items, unpinning buffer pages, and freeing allocated memory.

## Definition


## Detailed Description
The btendscan function performs cleanup operations when ending a B-tree index scan. It handles any remaining killed items by calling _bt_killitems, unpins any pinned buffer pages to release locks, and systematically frees all dynamically allocated memory associated with the scan including scan keys, array contexts, killed items arrays, and tuple workspaces. The function ensures that no resources are leaked when a scan operation completes.

## Parameters / Member Variables
- : The IndexScanDesc structure representing the scan to be terminated

## Dependencies
- Functions called/Symbols referenced:
  - BTScanPosIsValid
  - [_bt_killitems](_bt_killitems.md)
  - BTScanPosUnpinIfPinned
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [IndexScanDesc](../I/IndexScanDesc.md)
  - BTScanOpaque
- Called from (representative examples):
  - [bthandler](bthandler.md)

## Notes and Other Information
- Processes any remaining killed items before releasing the current page to maintain index consistency
- The markTuples workspace is not explicitly freed because it shares the same memory allocation as currTuples (allocated as one block in btrescan)
- Array-related memory (arrayKeys and orderProcs) is freed by deleting the arrayContext memory context
- Position invalidation is skipped since the entire scan structure will be freed
- Ensures all buffer pins are released to avoid holding unnecessary locks on index pages