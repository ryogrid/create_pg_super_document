# btmarkpos

## Location
src/backend/access/nbtree/nbtree.c: 453 - 478

## Overview
Saves the current position in a B-tree index scan to enable later restoration to this position via btrestrpos.

## Definition


## Detailed Description
The btmarkpos function implements a lightweight position marking mechanism for B-tree index scans. Instead of immediately copying the entire scan position structure, it uses a lazy approach that only records the current item index. If the scan moves to a different page before the mark is restored or moved, the full position structure will be copied by _bt_steppage. This optimization avoids unnecessary copying when marks are frequently moved within the same page, which is a common usage pattern.

## Parameters / Member Variables
- : The IndexScanDesc structure representing the active scan whose position should be marked

## Dependencies
- Functions called/Symbols referenced:
  - BTScanPosUnpinIfPinned
  - BTScanPosIsValid
  - BTScanPosInvalidate
  - [IndexScanDesc](../I/IndexScanDesc.md)
  - BTScanOpaque
- Called from (representative examples):
  - [bthandler](bthandler.md)

## Notes and Other Information
- Uses lazy evaluation for efficiency: only stores itemIndex initially, full position copy is deferred until needed
- Unpins any previously marked position to release buffer references
- The actual copying of the full scan position occurs in _bt_steppage when crossing page boundaries
- Sets markItemIndex to -1 when the current position is invalid
- This approach optimizes for the common case where marks are moved frequently within the same index page