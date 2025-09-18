# BufferGetBlockNumber

## Location
[src/backend/storage/buffer/bufmgr.c:3713-3733](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3713-L3733)

## Overview
BufferGetBlockNumber returns the block number associated with a specified buffer, providing essential metadata for buffer management operations.

## Definition
```c
BlockNumber BufferGetBlockNumber(Buffer buffer)
```

## Detailed Description
BufferGetBlockNumber is a utility function that extracts the block number from a buffer's tag information. The function handles both shared and local buffers by using appropriate descriptor retrieval methods. It includes safety assertions to ensure the buffer is pinned before accessing its metadata, which prevents race conditions where the buffer content might change during access. Since the buffer must be pinned, the function can safely read the tag information without acquiring a spinlock, improving performance. The block number is a fundamental piece of metadata used throughout PostgreSQL's storage system for identifying specific disk blocks.

## Parameters / Member Variables
- `buffer`: The buffer whose block number is to be retrieved (can be either a shared or local buffer)

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsPinned
  - BufferIsLocal
  - [GetLocalBufferDescriptor](../G/GetLocalBufferDescriptor.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [BufferDesc](BufferDesc.md)
- Called from (representative examples):
  - [brinbuild](../b/brinbuild.md)
  - [terminate_brin_buildstate](../t/terminate_brin_buildstate.md)
  - [_bt_doinsert](../b/_bt_doinsert.md)
  - [heap_insert](../h/heap_insert.md)
  - [visibilitymap_clear](../v/visibilitymap_clear.md)

## Notes and Other Information
- Requires the buffer to be pinned before calling (enforced by assertion)
- Handles both local and shared buffers appropriately
- Reads tag information without spinlock since buffer is pinned
- Returns BlockNumber type which represents the physical block number on disk
- Critical for buffer management and storage operations throughout PostgreSQL