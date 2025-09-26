# ReleaseBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:4897-4913](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4897-L4913)

## Overview
ReleaseBuffer releases the pin on a buffer, providing the primary interface for unpinning both shared and local buffers in PostgreSQL's buffer management system.

## Definition
```c
void ReleaseBuffer(Buffer buffer)
```

## Detailed Description
This function serves as the main interface for releasing buffer pins in PostgreSQL. It handles both shared and local buffers appropriately, performing validation and delegating to the appropriate unpinning function. The operation includes:

- Validation that the buffer identifier is valid
- Distinguishing between local and shared buffers
- Delegating to UnpinLocalBuffer for local buffers
- Delegating to UnpinBuffer for shared buffers after converting the buffer ID to a buffer descriptor

This function is essential for proper buffer management and resource cleanup, ensuring that buffer pins are properly released when no longer needed.

## Parameters / Member Variables
- `buffer`: Buffer identifier to be released (can be either shared or local buffer)

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsValid, BufferIsLocal
  - UnpinLocalBuffer
  - UnpinBuffer
  - GetBufferDescriptor
- Called from (representative examples):
  - brininsert
  - heap_fetch
  - _bt_relbuf
  - XLogReadBufferExtended
  - UnlockReleaseBuffer

## Notes and Other Information
- This is the primary public interface for releasing buffer pins in PostgreSQL
- Performs error checking to ensure buffer validity before attempting to release
- Handles both shared and local buffers transparently to the caller
- The function is widely used throughout the codebase for proper resource management
- Buffer IDs are 1-based for shared buffers, requiring conversion to 0-based descriptor index
- Local buffers have separate handling through UnpinLocalBuffer
- Essential for preventing buffer leaks and ensuring proper buffer pool management
- Used extensively in access methods, WAL recovery, and executor operations