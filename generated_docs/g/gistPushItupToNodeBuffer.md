# gistPushItupToNodeBuffer

## Location
[src/backend/access/gist/gistbuildbuffers.c:336-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuildbuffers.c#L336-L405)

## Overview
Adds an index tuple to a node buffer, managing page allocation, disk I/O, and buffer overflow handling during GiST index construction.

## Definition

```c
void
gistPushItupToNodeBuffer(GISTBuildBuffers *gfbb, GISTNodeBuffer *nodeBuffer,
						 IndexTuple itup)
```
## Detailed Description
This function is the main entry point for adding index tuples to node buffers during GiST index builds. It handles several complex operations: initializing empty buffers with their first page, loading existing pages from disk when needed, managing page overflow by writing full pages to disk and allocating new ones, and monitoring buffer capacity to trigger emptying when buffers become half-full. The function also manages memory contexts to ensure allocations occur in the appropriate persistent context.

## Parameters / Member Variables
- : Pointer to GISTBuildBuffers structure containing the build state and temporary file management
- : Pointer to the target GISTNodeBuffer where the tuple should be added
- : The IndexTuple to be added to the buffer

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [gistAllocateNewPageBuffer](gistAllocateNewPageBuffer.md)
  - [gistAddLoadedBuffer](gistAddLoadedBuffer.md)
  - [gistLoadNodeBuffer](gistLoadNodeBuffer.md)
  - PAGE_NO_SPACE (macro)
  - [gistBuffersGetFreeBlock](gistBuffersGetFreeBlock.md)
  - [WriteTempFileBlock](../W/WriteTempFileBlock.md)
  - PAGE_FREE_SPACE (macro)
  - [gistPlaceItupToPage](gistPlaceItupToPage.md)
  - BUFFER_HALF_FILLED (macro)
  - [lcons](../l/lcons.md)
- Called from (representative examples):
  - [gistProcessItup](gistProcessItup.md)
  - [gistRelocateBuildBuffersOnSplit](gistRelocateBuildBuffersOnSplit.md)

## Notes and Other Information
- This is a public function used throughout the GiST build process
- Handles memory context switching to ensure allocations are in the persistent build context
- Automatically initializes empty buffers by creating their first page
- Loads pages from disk on-demand when the current page buffer is not in memory
- Implements page overflow handling by writing full pages to disk and creating new ones
- Maintains a linked list of pages using prev pointers for navigation
- Adds buffers to the emptying queue when they become half-full to manage memory pressure
- Essential for the buffering strategy that allows building large GiST indexes efficiently