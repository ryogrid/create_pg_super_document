# MemoryContextAllocAligned

## Location
[src/backend/utils/mmgr/mcxt.c:1408-1509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1408-L1509)

## Overview
Allocates memory from a specified memory context with custom byte alignment requirements, implementing alignment through additional memory allocation and pointer redirection.

## Definition

```c
void *
MemoryContextAllocAligned(MemoryContext context,
						  Size size, Size alignto, int flags)
```
## Detailed Description
The `MemoryContextAllocAligned` function provides memory allocation with custom alignment requirements from a specified memory context. It addresses the need for memory aligned to boundaries larger than the standard `MAXIMUM_ALIGNOF` guarantee provided by regular memory allocation functions.

The implementation works by allocating extra memory beyond the requested size to accommodate both the alignment requirements and a redirection `MemoryChunk` header. This redirection mechanism is essential for memory management operations like `pfree` and `repalloc`, as it allows these functions to locate the original unaligned memory chunk that was actually allocated by the underlying memory context.

The function stores alignment information in the redirection header and uses the block offset field to point back to the original unaligned chunk. This design ensures compatibility with PostgreSQL's memory management infrastructure while providing the necessary alignment guarantees.

## Parameters / Member Variables
- `context`: The memory context from which to allocate memory
- `size`: The size in bytes of memory to allocate
- `alignto`: The alignment boundary in bytes (must be a power of 2)
- `flags`: Control flags for allocation behavior (same as `MemoryContextAllocExtended`)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocExtended](MemoryContextAllocExtended.md)
  - PallocAlignedExtraBytes
  - TYPEALIGN
  - PointerGetMemoryChunk
  - [MemoryChunkSetHdrMask](MemoryChunkSetHdrMask.md)
  - MCTX_ALIGNED_REDIRECT_ID
  - [set_sentinel](../s/set_sentinel.md) (when MEMORY_CONTEXT_CHECKING enabled)
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from (representative examples):
  - [PageSetChecksumCopy](../P/PageSetChecksumCopy.md)
  - [smgr_bulk_get_buf](../s/smgr_bulk_get_buf.md)
  - [AlignedAllocRealloc](../A/AlignedAllocRealloc.md)
  - [palloc_aligned](../p/palloc_aligned.md)

## Notes and Other Information
- The `alignto` parameter must be a power of 2 and less than 128MB
- For alignments less than or equal to `MAXIMUM_ALIGNOF`, the function delegates to standard `MemoryContextAllocExtended`
- The implementation may not work with all memory context types (e.g., Slab contexts have size restrictions)
- Uses a redirection `MemoryChunk` to maintain compatibility with PostgreSQL's memory management system
- Includes Valgrind integration for memory debugging by marking appropriate regions as no-access
- When `MEMORY_CONTEXT_CHECKING` is enabled, includes sentinel bytes and requested size tracking
- The function performs several alignment and size validation assertions
- Located in src/backend/utils/mmgr/mcxt.c at lines 1408-1509

## Simplified Source

```c
// Allocate memory with custom alignment requirements
void *MemoryContextAllocAligned(MemoryContext context, Size size, Size alignto, int flags)
{
    MemoryChunk *alignedchunk;
    Size alloc_size;
    void *unaligned;
    void *aligned;

    // Validation: alignment must be reasonable and power of 2
    Assert(alignto < (128 * 1024 * 1024));
    Assert((alignto & (alignto - 1)) == 0);

    // Use standard allocation if alignment requirements are already met
    if (unlikely(alignto <= MAXIMUM_ALIGNOF))
        return MemoryContextAllocExtended(context, size, flags);

    // Calculate total allocation size needed for alignment + redirection header
    alloc_size = size + PallocAlignedExtraBytes(alignto);

#ifdef MEMORY_CONTEXT_CHECKING
    alloc_size += 1;  // Space for sentinel byte
#endif

    // Allocate the actual memory (unaligned)
    unaligned = MemoryContextAllocExtended(context, alloc_size, flags);

    // Calculate the aligned pointer position
    aligned = (void *) TYPEALIGN(alignto, (char *) unaligned + sizeof(MemoryChunk));

    // Set up redirection chunk for memory management compatibility
    alignedchunk = PointerGetMemoryChunk(aligned);
    MemoryChunkSetHdrMask(alignedchunk, unaligned, alignto, MCTX_ALIGNED_REDIRECT_ID);

    // Verify alignment is correct
    Assert((void *) TYPEALIGN(alignto, aligned) == aligned);

#ifdef MEMORY_CONTEXT_CHECKING
    alignedchunk->requested_size = size;
    set_sentinel(aligned, size);
#endif

    // Mark unused memory regions for Valgrind
    VALGRIND_MAKE_MEM_NOACCESS(unaligned, (char *) alignedchunk - (char *) unaligned);
    VALGRIND_MAKE_MEM_NOACCESS(alignedchunk, sizeof(MemoryChunk));

    return aligned;
}
```