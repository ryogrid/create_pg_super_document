# gistUnloadNodeBuffers

## Location
[src/backend/access/gist/gistbuildbuffers.c:272-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuildbuffers.c#L272-L287)

## Overview
Writes the last pages of all loaded node buffers to disk, effectively flushing all buffered data during GiST index construction.

## Definition


## Detailed Description
This function orchestrates the unloading of all node buffers that currently have pages loaded in memory. It iterates through the array of loaded buffers and calls gistUnloadNodeBuffer for each one, ensuring that all buffered index data is persisted to temporary storage. After unloading, it resets the loaded buffer count to zero, indicating that no buffers currently have active pages in memory.

## Parameters / Member Variables
- : Pointer to GISTBuildBuffers structure containing the array of loaded buffers and management state

## Dependencies
- Functions called/Symbols referenced:
  - [gistUnloadNodeBuffer](gistUnloadNodeBuffer.md)
- Called from (representative examples):
  - gistProcessEmptyingQueue

## Notes and Other Information
- This is a public function (not static) used by the GiST build process
- The function processes all buffers in the loadedBuffers array up to loadedBuffersCount
- After completion, loadedBuffersCount is reset to 0, indicating no buffers have active pages
- This is typically called when memory pressure requires flushing all buffered data
- Essential for managing memory usage during large index builds by ensuring buffered data is persisted