# visibilitymap_set

## Location
src/backend/access/heap/visibilitymap.c: 244 - 335

## Overview
Sets visibility map bits for a previously pinned page, handling WAL logging and maintaining consistency between heap pages and their visibility metadata.

## Definition


## Detailed Description
The visibilitymap_set function completes the second phase of visibility map bit setting operations. It sets the specified visibility bits in a pre-pinned visibility map page, ensuring proper WAL logging for crash recovery and Hot Standby support. The function includes comprehensive validation to ensure data integrity and handles different scenarios for normal operation versus recovery.

The function performs critical section protection during bit manipulation and handles WAL logging with special considerations for data checksums and hint bits. When setting all-visible bits, it ensures the corresponding heap page has the PD_ALL_VISIBLE bit set before proceeding. The cutoff_xid parameter supports Hot Standby by providing the oldest transaction ID that can see all tuples on the page.

## Parameters / Member Variables
- : The relation whose visibility map is being updated  
- : Block number of the heap page whose visibility bits are being set
- : Buffer containing the heap page (required for WAL logging, except in recovery)
- : LSN of the XLOG record being replayed (InvalidXLogRecPtr in normal operation)
- : Pre-pinned buffer containing the correct visibility map page
- : Largest xmin on the page (for Hot Standby, can be InvalidTransactionId)
- : Bitmask specifying which visibility bits to set

## Dependencies
- Functions called/Symbols referenced:
  - HEAPBLK_TO_MAPBLOCK/HEAPBLK_TO_MAPBYTE/HEAPBLK_TO_OFFSET (heap-to-map conversion macros)
  - BufferGetBlockNumber (gets block number from buffer)
  - PageGetContents (gets page contents from buffer)
  - PageIsAllVisible (checks if heap page has all-visible bit set)
  - log_heap_visible (generates WAL record for visibility changes)
  - XLogHintBitIsNeeded (determines if hint bit protection is needed)
  - RelationNeedsWAL (checks if relation requires WAL logging)
- Called from (representative examples):
  - heap_multi_insert (sets all-visible bits after bulk inserts)
  - lazy_scan_prune (sets visibility bits during vacuum operations)
  - heap_xlog_visible (sets bits during WAL replay)

## Notes and Other Information  
- Must be called with buffers previously pinned via visibilitymap_pin
- Requires heap page to have PD_ALL_VISIBLE bit set before calling (except in recovery)
- Handles both normal operation (generates WAL) and recovery mode (replays existing WAL)
- Uses critical sections to ensure atomicity of visibility map updates
- Special handling for data checksums: updates heap page LSN only when hint bits are protected
- Validates that all_frozen bit is never set without all_visible bit
- No-op if the requested bits are already set