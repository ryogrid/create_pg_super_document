# AllocSetGetChunkSpace

## Location
[src/backend/utils/mmgr/aset.c:1462-1495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/aset.c#L1462-L1495)

## Overview
Determines the total space occupied by a currently-allocated memory chunk, including all memory-allocation overhead.

## Definition
```c
Size AllocSetGetChunkSpace(void *pointer)
```

## Detailed Description
AllocSetGetChunkSpace calculates the total memory space used by an allocated chunk by examining the chunk header and determining its size based on whether it's a regular or external chunk. For external chunks, it calculates the space from the chunk start to the block's end pointer. For regular chunks, it uses the free list index stored in the chunk to determine the allocated size and adds the chunk header size. This function is essential for memory usage tracking and debugging.

## Parameters / Member Variables
- `pointer`: A void pointer to the allocated memory chunk whose total space needs to be determined

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk
  - [MemoryChunkIsExternal](../M/MemoryChunkIsExternal.md)
  - ExternalChunkGetBlock
  - [MemoryChunkGetValue](../M/MemoryChunkGetValue.md)
  - AllocBlockIsValid
  - FreeListIdxIsValid
  - GetChunkSizeFromFreeListIdx
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from (representative examples):
  - BOGUS_MCTX (via function pointer assignment)
  - Referenced in memutils_internal.h

## Notes and Other Information
- Returns total space including chunk header overhead (ALLOC_CHUNKHDRSZ)
- Handles both regular chunks (using free list index) and external chunks (using block end pointer)
- Uses Valgrind annotations to control memory access during chunk inspection
- Includes validation assertions for block and free list index integrity
- Part of PostgreSQL's memory context system for tracking memory usage