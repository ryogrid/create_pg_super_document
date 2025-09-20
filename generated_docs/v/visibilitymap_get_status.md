# visibilitymap_get_status

## Location
[src/backend/access/heap/visibilitymap.c:336-383](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/visibilitymap.c#L336-L383)

## Overview
Retrieves the current visibility status bits for a given heap block, indicating whether all tuples are visible to all transactions or marked as frozen.

## Definition

```c
uint8
visibilitymap_get_status(Relation rel, BlockNumber heapBlk, Buffer *vmbuf)
```
## Detailed Description
The visibilitymap_get_status function reads the visibility map to determine the current status of a heap page. It returns a bitmask indicating whether the page is all-visible, all-frozen, or has other visibility properties. The function optimizes buffer usage by reusing pinned buffers when possible and handles cases where the visibility map page doesn't exist yet.

This function is designed to be called without locks on the heap page, making it suitable for concurrent access scenarios. However, this means the returned status may be stale by the time the caller uses it, so the caller must handle potential race conditions. The function performs atomic single-byte reads from the visibility map to minimize locking overhead.

## Parameters / Member Variables
- : The relation whose visibility map is being queried
- : Block number of the heap page whose visibility status is requested
- : Pointer to buffer variable for visibility map page (input/output parameter)

## Dependencies
- Functions called/Symbols referenced:
  - HEAPBLK_TO_MAPBLOCK/HEAPBLK_TO_MAPBYTE/HEAPBLK_TO_OFFSET (heap-to-map conversion macros)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (gets block number from buffer)
  - ReleaseBuffer (releases buffer when wrong page is pinned)
  - [vm_readbuf](vm_readbuf.md) (reads visibility map page, with extend=false)
  - [PageGetContents](../P/PageGetContents.md) (gets page contents from buffer)
  - VISIBILITYMAP_VALID_BITS (mask for extracting valid visibility bits)
- Called from (representative examples):
  - [find_next_unskippable_block](../f/find_next_unskippable_block.md) (checks if vacuum can skip pages)
  - [lazy_scan_prune](../l/lazy_scan_prune.md) (determines page visibility status during vacuum)
  - VM_ALL_VISIBLE/VM_ALL_FROZEN (macro expansions for visibility checks)

## Notes and Other Information
- Returns false (0) if the visibility map page doesn't exist yet
- Optimizes performance by reusing existing pinned buffers when appropriate
- Performs lockless reads for better concurrency, but caller must handle race conditions
- The returned buffer remains pinned and must be released by the caller
- Single-byte reads are atomic, but memory ordering effects are caller's responsibility
- May return stale data due to concurrent modifications - this is by design for performance