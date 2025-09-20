# visibilitymap_prepare_truncate

## Location
[src/backend/access/heap/visibilitymap.c:438-537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/visibilitymap.c#L438-L537)

## Overview
Prepares the visibility map for truncation by calculating the new size and clearing unused bits in the last remaining map page.

## Definition

```c
BlockNumber
visibilitymap_prepare_truncate(Relation rel, BlockNumber nheapblocks)
```
## Detailed Description
This function prepares the visibility map for truncation to match a new heap size. It calculates which visibility map blocks will remain after truncation and ensures that unused bits in the last remaining map page are properly cleared. The function handles the case where the truncation doesn't fall exactly on a map page boundary by clearing tail bits that represent truncated heap blocks. If the visibility map doesn't exist or is already smaller than the requested size, it returns InvalidBlockNumber. The function also handles WAL logging requirements when checksums are enabled.

## Parameters / Member Variables
- : The relation whose visibility map should be prepared for truncation
- : The new size of the heap in blocks

## Dependencies
- Functions called/Symbols referenced:
  - HEAPBLK_TO_MAPBLOCK
  - HEAPBLK_TO_MAPBYTE
  - HEAPBLK_TO_OFFSET
  - [smgrexists](../s/smgrexists.md)
  - RelationGetSmgr
  - [vm_readbuf](vm_readbuf.md)
  - [PageGetContents](../P/PageGetContents.md)
  - [LockBuffer](../L/LockBuffer.md)
  - MemSet
  - MarkBufferDirty
  - RelationNeedsWAL
  - XLogHintBitIsNeeded
  - [log_newpage_buffer](../l/log_newpage_buffer.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - smgrnblocks
  - VISIBILITYMAP_FORKNUM
  - MAPSIZE
- Called from (representative examples):
  - [RelationTruncate](../R/RelationTruncate.md)
  - [smgr_redo](../s/smgr_redo.md)

## Notes and Other Information
- Returns InvalidBlockNumber if no truncation is needed, otherwise returns the number of blocks in the new visibility map
- Uses critical sections and proper locking when modifying map pages
- Handles WAL logging for torn page protection when checksums are enabled
- Includes detailed bit masking logic to clear unwanted bits in the last byte of the truncated map
- Part of PostgreSQL's relation truncation process