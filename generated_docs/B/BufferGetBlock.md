# BufferGetBlock

## Location
[src/include/storage/bufmgr.h:371-392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufmgr.h#L371-L392)

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
  - [BufferIsValid](BufferIsValid.md) (validation function)
  - BufferIsLocal (function to check if buffer is local)
  - LocalBufferBlockPointers (array of local buffer pointers)
  - BufferBlocks (base address of shared buffer blocks)
  - BLCKSZ (block size constant)
  - Size (type alias)
  - Block (return type)
- Called from (representative examples):
  - [XLogSaveBufferForHint](../X/XLogSaveBufferForHint.md) (src/backend/access/transam/xloginsert.c:1093)
  - [WaitReadBuffers](../W/WaitReadBuffers.md) (src/backend/storage/buffer/bufmgr.c:1480, 1499)
  - [BufferGetPage](BufferGetPage.md) (src/include/storage/bufmgr.h:406)

## Notes and Other Information
- The function assumes the buffer parameter is valid and will assert if it is not
- Local buffers use negative indexing and are stored separately from shared buffers
- Shared buffers are stored in a contiguous memory region starting at BufferBlocks
- The returned Block is a pointer to the actual page data that can be read or modified
- This is a low-level function that provides direct access to buffer memory

## Simplified Source

```c
static inline Block
BufferGetBlock(Buffer buffer)
{
    Assert(BufferIsValid(buffer));

    // Handle local buffers (negative buffer numbers)
    if (BufferIsLocal(buffer))
        return LocalBufferBlockPointers[-buffer - 1];
    else
        // Handle shared buffers (positive buffer numbers)
        return (Block) (BufferBlocks + ((Size) (buffer - 1)) * BLCKSZ);
}
```