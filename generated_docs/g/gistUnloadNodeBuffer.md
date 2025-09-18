# gistUnloadNodeBuffer

## Location
src/backend/access/gist/gistbuildbuffers.c: 246 - 271

## Overview
Writes the last page of a node buffer to disk during GiST index construction, flushing buffered data to temporary storage.

## Definition


## Detailed Description
This function is responsible for persisting the buffered page data of a GiST node buffer to disk. It's called as part of the buffer management strategy during GiST index builds to free memory by writing accumulated index tuples to temporary storage. The function allocates a free block in the temporary file, writes the page buffer contents, and updates the node buffer's metadata to track the disk location.

## Parameters / Member Variables
- : Pointer to GISTBuildBuffers structure containing the temporary file and buffer management state
- : Pointer to the GISTNodeBuffer whose page buffer needs to be written to disk

## Dependencies
- Functions called/Symbols referenced:
  - gistBuffersGetFreeBlock
  - WriteTempFileBlock
  - pfree
- Called from (representative examples):
  - gistUnloadNodeBuffers

## Notes and Other Information
- This is a static function, only accessible within the gistbuildbuffers.c file
- The function only performs I/O if the node buffer actually contains data (pageBuffer is not NULL)
- After writing to disk, the page buffer memory is freed and the pointer is set to NULL
- The block number where data was written is stored in nodeBuffer->pageBlocknum for later retrieval
- This is part of the memory management strategy to prevent excessive memory usage during large index builds