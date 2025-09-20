# visibilitymap_pin

## Location
[src/backend/access/heap/visibilitymap.c:191-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/visibilitymap.c#L191-L214)

## Overview
Pins a visibility map page in memory to prepare for setting bits, handling I/O operations and buffer management required to access the correct map page.

## Definition

```c
void
visibilitymap_pin(Relation rel, BlockNumber heapBlk, Buffer *vmbuf)
```
## Detailed Description
The visibilitymap_pin function implements the first phase of a two-phase operation for setting visibility map bits. It pins the visibility map page that contains the bit corresponding to a given heap block number. This separation allows I/O operations to occur without holding locks on heap pages, improving concurrency.

The function optimizes performance by reusing existing pinned buffers when possible. If the provided buffer already contains the correct map page, no additional I/O is performed. If a different page is needed, the old buffer is released and a new one is obtained. If the required page doesn't exist in the map file, the file is extended automatically.

## Parameters / Member Variables
- : The relation whose visibility map page needs to be pinned
- : Block number of the heap page for which the visibility bit will be set
- : Pointer to buffer variable; input can be InvalidBuffer or previously pinned buffer, output is the correctly pinned buffer

## Dependencies
- Functions called/Symbols referenced:
  - HEAPBLK_TO_MAPBLOCK (macro for converting heap block to map block)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (gets block number from buffer)
  - ReleaseBuffer (releases previously pinned buffer)
  - [vm_readbuf](vm_readbuf.md) (internal function to read/extend visibility map pages)
- Called from (representative examples):
  - [heap_delete](../h/heap_delete.md) (pins before clearing bits during tuple deletion)
  - [heap_update](../h/heap_update.md) (pins before setting/clearing bits during tuple updates)
  - [RelationGetBufferForTuple](../R/RelationGetBufferForTuple.md) (pins during tuple insertion operations)
  - [lazy_scan_heap](../l/lazy_scan_heap.md) (pins during vacuum operations)

## Notes and Other Information
- Part of a two-phase protocol: pin first, then set bits with visibilitymap_set
- Automatically extends the visibility map file if the required page doesn't exist
- Optimizes for repeated operations on the same map page by reusing buffers
- Should not be called while holding locks on heap pages due to potential I/O
- The returned buffer remains pinned until explicitly released or reused