# BufferGetBlock

## Location
src/include/storage/bufmgr.h: 371 - 392

## Overview
BufferGetBlock is a static inline function that returns a reference to a disk page image associated with a buffer in PostgreSQLs buffer management system.

## Definition
static inline Block BufferGetBlock(Buffer buffer)

## Detailed Description
BufferGetBlock retrieves the actual memory block (disk page image) associated with a given buffer identifier. The function handles both local and shared buffers differently:

For local buffers (negative buffer numbers), it indexes into the LocalBufferBlockPointers array using the formula: LocalBufferBlockPointers[-buffer - 1].

For shared buffers (positive buffer numbers), it calculates the memory address by offsetting from BufferBlocks using the formula: BufferBlocks + ((Size)(buffer - 1)) * BLCKSZ, where BLCKSZ is the block size constant.

The function assumes the buffer is valid and includes an assertion to verify this precondition.

## Parameters / Member Variables
- buffer: Buffer identifier for which to retrieve the block pointer (type: Buffer)

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsValid (validation function)
  - BufferIsLocal (function to check if buffer is local)
  - LocalBufferBlockPointers (array of local buffer pointers)
  - BufferBlocks (base address of shared buffer blocks)
  - BLCKSZ (block size constant)
  - Size (type alias)
  - Block (return type)
- Called from (representative examples):
  - XLogSaveBufferForHint (src/backend/access/transam/xloginsert.c:1093)
  - WaitReadBuffers (src/backend/storage/buffer/bufmgr.c:1480, 1499)
  - BufferGetPage (src/include/storage/bufmgr.h:406)

## Notes and Other Information
- The function assumes the buffer parameter is valid and will assert if it is not
- Local buffers use negative indexing and are stored separately from shared buffers
- Shared buffers are stored in a contiguous memory region starting at BufferBlocks
- The returned Block is a pointer to the actual page data that can be read or modified
- This is a low-level function that provides direct access to buffer memory