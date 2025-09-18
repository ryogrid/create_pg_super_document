# visibilitymap_pin_ok

## Location
[src/backend/access/heap/visibilitymap.c:215-243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/visibilitymap.c#L215-L243)

## Overview
Checks whether a currently pinned buffer contains the correct visibility map page for a given heap block number.

## Definition


## Detailed Description
The visibilitymap_pin_ok function provides a lightweight check to determine if a buffer that's already pinned contains the visibility map page needed for a specific heap block. This function is primarily used for optimization purposes to avoid unnecessary I/O operations when the correct page is already available in memory.

The function performs a simple validation: it converts the heap block number to the corresponding map block number and compares it with the block number of the pinned buffer. This allows callers to efficiently determine whether they can reuse an existing pinned buffer or need to pin a different page.

## Parameters / Member Variables
- : Block number of the heap page whose visibility map page is being checked
- : Buffer that may contain the correct visibility map page (can be InvalidBuffer)

## Dependencies
- Functions called/Symbols referenced:
  - HEAPBLK_TO_MAPBLOCK (macro for converting heap block to map block)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (gets block number from buffer)
- Called from (representative examples):
  - [heap_multi_insert](../h/heap_multi_insert.md) (checks if correct page is pinned during bulk inserts)
  - [GetVisibilityMapPins](../G/GetVisibilityMapPins.md) (optimizes buffer management during tuple operations)
  - [RelationGetBufferForTuple](../R/RelationGetBufferForTuple.md) (avoids redundant pinning during tuple insertion)

## Notes and Other Information
- Returns true if the buffer is valid and contains the correct map page, false otherwise
- Lightweight function designed for performance optimization in hot code paths
- Does not perform any I/O or buffer management operations
- Safe to call with InvalidBuffer (will return false)
- Typically used before calling visibilitymap_pin to avoid redundant operations