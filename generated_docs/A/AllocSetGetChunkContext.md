# AllocSetGetChunkContext

## Location
[src/backend/utils/mmgr/aset.c:1433-1461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/aset.c#L1433-L1461)

## Overview
Returns the MemoryContext that a given memory pointer belongs to, allowing retrieval of context information from allocated memory chunks.

## Definition

```c
MemoryContext
AllocSetGetChunkContext(void *pointer)
```
## Detailed Description
AllocSetGetChunkContext takes a memory pointer and returns the associated MemoryContext by traversing from the memory chunk header to its containing memory block and then to the AllocSet context. This function enables context identification for debugging and memory management purposes. It handles both regular memory chunks and external chunks, using appropriate Valgrind annotations to control memory access validation during the process.

## Parameters / Member Variables
- : A void pointer to allocated memory whose containing MemoryContext needs to be determined

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk
  - [MemoryChunkIsExternal](../M/MemoryChunkIsExternal.md)
  - ExternalChunkGetBlock
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - AllocBlockIsValid
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from (representative examples):
  - BOGUS_MCTX (via function pointer assignment)
  - Referenced in memutils_internal.h

## Notes and Other Information
- Uses Valgrind macros to temporarily allow access to chunk headers during context retrieval
- Handles both regular and external memory chunks appropriately  
- Includes assertion to validate the memory block integrity
- Part of the AllocSet memory context implementation in PostgreSQL's memory management system