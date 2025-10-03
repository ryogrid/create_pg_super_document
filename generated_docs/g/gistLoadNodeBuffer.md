# gistLoadNodeBuffer

## Location
[src/backend/access/gist/gistbuildbuffers.c:221-245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuildbuffers.c#L221-L245)

## Overview
gistLoadNodeBuffer loads the last page of a node buffer from temporary file storage into main memory during GiST index construction.

## Definition

```c
static void
gistLoadNodeBuffer(GISTBuildBuffers *gfbb, GISTNodeBuffer *nodeBuffer)
```
## Detailed Description
This static function manages the transition of node buffer pages from disk-based temporary storage back into main memory. It performs a conditional load operation - only loading pages when the buffer doesn't already have a page in memory and when there are actually blocks associated with the buffer.

The loading process involves several coordinated steps:
1. Allocates a new page buffer in memory using gistAllocateNewPageBuffer
2. Reads the buffer's last page from the temporary file using ReadTempFileBlock
3. Marks the temporary file block as free for reuse via gistBuffersReleaseBlock  
4. Registers the buffer in the loadedBuffers tracking array
5. Resets the pageBlocknum to InvalidBlockNumber to indicate the page is now in memory

This function is crucial for the buffer management system's ability to swap pages between memory and temporary storage, enabling construction of large indexes that exceed available memory.

## Parameters / Member Variables
- : The GiST build buffers structure containing temporary file and buffer management data
- : The node buffer whose last page should be loaded into memory

## Dependencies
- Functions called/Symbols referenced:
  - [gistAllocateNewPageBuffer](gistAllocateNewPageBuffer.md)
  - [ReadTempFileBlock](../R/ReadTempFileBlock.md)  
  - [gistBuffersReleaseBlock](gistBuffersReleaseBlock.md)
  - [gistAddLoadedBuffer](gistAddLoadedBuffer.md)
  - InvalidBlockNumber
- Called from (representative examples):
  - [gistPushItupToNodeBuffer](gistPushItupToNodeBuffer.md)
  - [gistPopItupFromNodeBuffer](gistPopItupFromNodeBuffer.md)

## Notes and Other Information
- Function is declared static, making it internal to the gistbuildbuffers.c module
- Only loads pages when pageBuffer is NULL and blocksCount > 0, avoiding unnecessary operations
- Coordinates memory allocation, file I/O, and buffer tracking in a single atomic operation
- Maintains consistency by immediately marking the file block as free after loading
- Uses the established ReadTempFileBlock function (referenced in related symbols) for file I/O
- The pageBlocknum reset to InvalidBlockNumber serves as a flag indicating the page is memory-resident
- Essential for the lazy-loading strategy that enables efficient memory usage during large index builds

## Simplified Source

```c
static void
gistLoadNodeBuffer(GISTBuildBuffers *gfbb, GISTNodeBuffer *nodeBuffer)
{
    // Only load if buffer has data but no page in memory
    if (!nodeBuffer->pageBuffer && nodeBuffer->blocksCount > 0)
    {
        // Allocate memory for page
        nodeBuffer->pageBuffer = gistAllocateNewPageBuffer(gfbb);

        // Read page from temporary file
        ReadTempFileBlock(gfbb->pfile, nodeBuffer->pageBlocknum,
                         nodeBuffer->pageBuffer);

        // Mark file block as free for reuse
        gistBuffersReleaseBlock(gfbb, nodeBuffer->pageBlocknum);

        // Track loaded buffer and reset page block number
        gistAddLoadedBuffer(gfbb, nodeBuffer);
        nodeBuffer->pageBlocknum = InvalidBlockNumber;
    }
}
```