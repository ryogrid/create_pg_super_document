# btrescan

## Location
src/backend/access/nbtree/nbtree.c: 359 - 416

## Overview
Resets and prepares a B-tree index scan with new scan keys, handling cleanup of previous scan state and initializing workspace for index-only scans when needed.

## Definition


## Detailed Description
The btrescan function reinitializes an existing B-tree index scan with new scan parameters. It performs cleanup operations including handling killed items from the previous scan, unpinning buffer pages, and invalidating scan positions. The function also allocates tuple workspace arrays for index-only scans if needed, using a single memory block for both current and mark tuple workspaces for efficiency. This function is called both for initial scan setup and when restarting a scan with different keys.

## Parameters / Member Variables
- : The IndexScanDesc structure representing the ongoing scan
- : Array of scan keys defining the scan conditions  
- : Number of scan keys in the scankey array
- : Array of order-by keys (unused for B-tree, should be NULL)
- : Number of order-by keys (should be 0 for B-tree)

## Dependencies
- Functions called/Symbols referenced:
  - BTScanPosIsValid
  - _bt_killitems
  - BTScanPosUnpinIfPinned
  - BTScanPosInvalidate
  - IndexScanDesc
  - ScanKey
  - BTScanOpaque
- Called from (representative examples):
  - bthandler

## Notes and Other Information
- Handles cleanup of killed items from previous scan iterations using _bt_killitems
- Allocates tuple workspace as a single BLCKSZ*2 block, with markTuples positioned after currTuples
- The tuple workspace allocation includes a safety mechanism for name_ops columns to prevent memory access violations
- Resets scan position markers and key counts, which will be properly set later by _bt_preprocess_keys
- Only allocates tuple workspace when xs_want_itup is true, indicating an index-only scan is desired