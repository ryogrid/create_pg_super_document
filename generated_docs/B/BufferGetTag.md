# BufferGetTag

## Location
[src/backend/storage/buffer/bufmgr.c:3734-3772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3734-L3772)

## Overview
BufferGetTag extracts the complete tag information from a buffer, including the relation file locator, fork number, and block number, providing comprehensive buffer identification metadata.

## Definition
```c
void BufferGetTag(Buffer buffer, RelFileLocator *rlocator, ForkNumber *forknum, BlockNumber *blknum)
```

## Detailed Description
BufferGetTag is a comprehensive buffer metadata extraction function that retrieves all the essential identification information stored in a buffer's tag. Unlike BufferGetBlockNumber which only returns the block number, this function provides complete buffer identification by extracting the relation file locator (which identifies the specific relation and tablespace), the fork number (indicating which fork of the relation), and the block number (identifying the specific block within that fork). The function follows the same safety pattern as BufferGetBlockNumber, requiring the buffer to be pinned and handling both local and shared buffers appropriately. This complete tag information is crucial for WAL logging, buffer management, and storage system operations.

## Parameters / Member Variables
- `buffer`: The buffer whose tag information is to be retrieved
- `rlocator`: Output parameter that receives the relation file locator
- `forknum`: Output parameter that receives the fork number  
- `blknum`: Output parameter that receives the block number

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsPinned
  - BufferIsLocal
  - [GetLocalBufferDescriptor](../G/GetLocalBufferDescriptor.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [BufTagGetRelFileLocator](BufTagGetRelFileLocator.md)
  - [BufTagGetForkNum](BufTagGetForkNum.md)
  - [BufferDesc](BufferDesc.md)
- Called from (representative examples):
  - [ginRedoInsertEntry](../g/ginRedoInsertEntry.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogSaveBufferForHint](../X/XLogSaveBufferForHint.md)
  - [log_newpage_buffer](../l/log_newpage_buffer.md)
  - fsm_search_avail

## Notes and Other Information
- Requires the buffer to be pinned before calling (enforced by assertion)
- Returns complete buffer identification through output parameters
- Uses helper macros to extract relfilelocator and fork number from the tag
- Critical for WAL logging operations that need complete buffer identification
- Handles both local and shared buffers using appropriate descriptor access methods