# SlabCheck

## Location
[src/backend/utils/mmgr/slab.c:997-1154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/slab.c#L997-L1154)

## Overview
SlabCheck performs comprehensive integrity validation of a Slab memory context by walking through all blocks and chunks to detect memory corruption and inconsistencies.

## Definition


## Detailed Description
SlabCheck is a diagnostic function that thoroughly validates the internal consistency of a Slab memory context. It performs multiple levels of validation including block list integrity, chunk accounting accuracy, free list consistency, and memory boundary checks. The function is designed to detect various forms of memory corruption including incorrect block placement, invalid free list links, chunk header corruption, and buffer overruns.

The validation process includes checking empty blocks for correct free chunk counts, verifying that blocks are placed on appropriate blocklists based on their free chunk count, validating free list pointers and chunk alignment, checking unused chunk tracking, and verifying chunk headers and sentinel bytes for allocated chunks. All errors are reported as WARNING level messages rather than ERROR or FATAL to prevent infinite recursion during error handling.

## Parameters / Member Variables
- : The MemoryContext to validate (cast internally to SlabContext)

## Dependencies
- Functions called/Symbols referenced:
  - SlabIsValid
  - dclist_foreach
  - dlist_container
  - [SlabBlocklistIndex](SlabBlocklistIndex.md)
  - SlabChunkIndex
  - SlabBlockGetChunk
  - SlabChunkMod
  - MemoryChunkGetPointer
  - SlabChunkGetPointer
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - [sentinel_ok](../s/sentinel_ok.md)
  - [dclist_count](../d/dclist_count.md)
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from (representative examples):
  - [SlabReset](SlabReset.md)
  - Memory context debugging routines

## Notes and Other Information
This function uses WARNING level logging rather than ERROR/FATAL to prevent infinite loops during error cleanup when memory corruption is detected. It maintains a temporary isChunkFree array to track chunk states during validation. The function performs Valgrind-aware memory access control, temporarily allowing access to chunk headers for validation then disabling access again. Sentinel byte checking helps detect buffer overruns in allocated chunks. This is a critical debugging tool for diagnosing Slab allocator issues and memory corruption problems in PostgreSQL.