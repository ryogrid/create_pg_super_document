# gistPopItupFromNodeBuffer

## Location
[src/backend/access/gist/gistbuildbuffers.c:406-467](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuildbuffers.c#L406-L467)

## Overview
Removes one index tuple from a GiST node buffer during index construction, managing buffer pages and memory allocation in the process.

## Definition

```c
bool
gistPopItupFromNodeBuffer(GISTBuildBuffers *gfbb, GISTNodeBuffer *nodeBuffer,
						  IndexTuple *itup)
```
## Detailed Description
This function is a core component of GiST index construction that removes index tuples from node buffers. It handles the complex logic of managing buffered pages during the build process, including loading pages from temporary files when needed, extracting tuples, and cleaning up empty pages. The function maintains the integrity of the buffer chain by properly handling page transitions and memory management.

When a page becomes empty after tuple removal, the function automatically fetches the previous page in the buffer chain and releases the emptied page's disk block for reuse. This ensures efficient memory and disk space utilization during index construction.

## Parameters / Member Variables
- : Pointer to the main GiST build buffers structure containing global build state
- : Pointer to the specific node buffer from which to remove a tuple
- : Output parameter that receives the removed index tuple

## Dependencies
- Functions called/Symbols referenced:
  - [gistLoadNodeBuffer](gistLoadNodeBuffer.md)
  - [gistGetItupFromPage](gistGetItupFromPage.md)
  - PAGE_IS_EMPTY
  - [ReadTempFileBlock](../R/ReadTempFileBlock.md)
  - [gistBuffersReleaseBlock](gistBuffersReleaseBlock.md)
- Called from (representative examples):
  - [gistProcessEmptyingQueue](gistProcessEmptyingQueue.md)
  - [gistRelocateBuildBuffersOnSplit](gistRelocateBuildBuffersOnSplit.md)

## Notes and Other Information
- Returns true if a tuple was successfully removed, false if the buffer is empty
- Automatically manages page transitions when the current page becomes empty
- Properly releases disk blocks for reuse to maintain efficient storage utilization
- Part of the GiST index build buffer management system that enables memory-efficient construction of large indexes
- The function maintains the backward-linked list structure of buffer pages

## Simplified Source

```c
bool gistPopItupFromNodeBuffer(GISTBuildBuffers *gfbb, GISTNodeBuffer *nodeBuffer, IndexTuple *itup) {
    // Return false if buffer is empty
    if (nodeBuffer->blocksCount <= 0)
        return false;

    // Load page buffer from disk if needed
    if (!nodeBuffer->pageBuffer)
        gistLoadNodeBuffer(gfbb, nodeBuffer);

    // Extract tuple from current page
    gistGetItupFromPage(nodeBuffer->pageBuffer, itup);

    // Handle page cleanup if it becomes empty
    if (PAGE_IS_EMPTY(nodeBuffer->pageBuffer)) {
        BlockNumber prevblkno = nodeBuffer->pageBuffer->prev;
        nodeBuffer->blocksCount--;

        if (prevblkno != InvalidBlockNumber) {
            // Load previous page and release current block
            ReadTempFileBlock(gfbb->pfile, prevblkno, nodeBuffer->pageBuffer);
            gistBuffersReleaseBlock(gfbb, prevblkno);
        } else {
            // No more pages - free memory
            pfree(nodeBuffer->pageBuffer);
            nodeBuffer->pageBuffer = NULL;
        }
    }

    return true;
}
```