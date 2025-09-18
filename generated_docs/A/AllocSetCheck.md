# AllocSetCheck

## Location
[src/backend/utils/mmgr/aset.c:1599-1724](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/aset.c#L1599-L1724)

## Overview
Performs comprehensive consistency checking of an AllocSet memory context by walking through all chunks and validating memory integrity.

## Definition
```c
void AllocSetCheck(MemoryContext context)
```

## Detailed Description
AllocSetCheck is a debugging and validation function that thoroughly examines an AllocSet memory context for consistency errors. It walks through all memory blocks and chunks, validating block headers, chunk sizes, alignment, and detecting memory corruption such as buffer overruns. The function checks that external chunks consume entire blocks, validates free list indices, ensures requested sizes don't exceed allocated sizes, and verifies block linkage. It uses Valgrind annotations during chunk inspection and reports all errors as WARNING level messages to avoid infinite loops during error recovery. The function is primarily used during context reset and deletion operations when MEMORY_CONTEXT_CHECKING is enabled.

## Parameters / Member Variables
- `context`: The MemoryContext to check for consistency and integrity

## Dependencies
- Functions called/Symbols referenced:
  - IsKeeperBlock
  - [MemoryChunkIsExternal](../M/MemoryChunkIsExternal.md)
  - MemoryChunkGetPointer
  - [MemoryChunkGetValue](../M/MemoryChunkGetValue.md)
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - FreeListIdxIsValid
  - GetChunkSizeFromFreeListIdx
  - [sentinel_ok](../s/sentinel_ok.md)
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
  - elog
- Called from (representative examples):
  - [AllocSetReset](AllocSetReset.md)
  - [AllocSetDelete](AllocSetDelete.md)
  - BOGUS_MCTX (via function pointer assignment)
  - Referenced in memutils_internal.h

## Notes and Other Information
- Reports errors as WARNING level to prevent infinite loops during error cleanup
- Validates block header fields including prev/next linkage and free pointer boundaries
- Checks external chunks consume entire blocks and regular chunks have valid free list indices
- Detects buffer overruns by checking sentinel values in padding space
- Ensures total allocated memory matches context's mem_allocated counter
- Only active when MEMORY_CONTEXT_CHECKING is enabled
- Part of PostgreSQL's memory debugging and validation infrastructure
- Critical for detecting memory corruption in development and testing environments