# hashendscan

## Location
src/backend/access/hash/hash.c: 431 - 461

## Overview
Terminates a hash index scan and cleans up all associated resources including buffers and memory allocations.

## Definition
```c
void hashendscan(IndexScanDesc scan)
```

## Detailed Description
The hashendscan function properly terminates a hash index scan operation by performing comprehensive cleanup of all resources allocated during the scan. This includes processing any remaining killed (dead) tuples, releasing buffer pins, and freeing memory allocations.

The function first checks if there is a valid current scan position, and if so, processes any tuples that were marked for killing during the scan by calling _hash_kill_items. This ensures that dead tuples are properly handled before the scan ends. It then calls _hash_dropscanbuf to release any buffer pins held by the scan.

Finally, it deallocates the memory used by the hash-specific scan state, including the killedItems array if it was allocated, and the entire HashScanOpaque structure. The opaque pointer in the main scan structure is set to NULL to indicate that the hash-specific data has been cleaned up.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure for the scan to be terminated

## Dependencies
- Functions called/Symbols referenced:
  - HashScanPosIsValid
  - [_hash_kill_items](_hash_kill_items.md)
  - [_hash_dropscanbuf](_hash_dropscanbuf.md)
  - [pfree](../p/pfree.md) (memory deallocation)
  - HashScanOpaque (structure type)
- Called from (representative examples):
  - [hashhandler](hashhandler.md) (hash access method handler)
  - Referenced in HASHNProcs (hash index procedure array)

## Notes and Other Information
- Ensures all killed tuples are processed before terminating the scan
- Releases all buffer pins held by the scan to prevent resource leaks
- Properly deallocates both the killedItems array and the main opaque structure
- Sets the opaque pointer to NULL to prevent accidental access after cleanup
- Essential for preventing memory and buffer leaks in long-running database sessions
- Should always be called when a scan is no longer needed