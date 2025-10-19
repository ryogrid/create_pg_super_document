# SlabRealloc

## Location
[src/backend/utils/mmgr/slab.c:826-862](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/slab.c#L826-L862)

## Overview
SlabRealloc is a limited reallocation function that only allows reallocation to the same size, as slab allocators are designed for fixed-size chunks.

## Definition
```c
void *SlabRealloc(void *pointer, Size size, int flags)
```

## Detailed Description
SlabRealloc provides a constrained implementation of memory reallocation for the slab allocator. Since slab allocators are specifically designed to manage equal-sized memory chunks efficiently, true reallocation (changing chunk size) is not supported. The function performs the following operations:

1. **Chunk Validation**: Retrieves and validates the memory chunk header from the given pointer
2. **Block Verification**: Ensures the chunk belongs to a valid slab block using test-and-elog for error handling
3. **Size Comparison**: Compares the requested size with the slab's fixed chunk size
4. **Conditional Return**: If the size matches exactly, returns the original pointer unchanged
5. **Error Handling**: Throws an ERROR if a different size is requested

The function is designed to be "gentle" by allowing realloc calls with the same size, which is a common pattern in some code paths, while preventing actual size changes that would violate slab allocator principles.

## Parameters / Member Variables
- `pointer`: The memory chunk to be reallocated, previously allocated by SlabAlloc
- `size`: The requested new size (only accepted if equal to current chunk size)
- `flags`: Allocation flags (currently unused in implementation)

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)  
  - SlabBlockIsValid
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from (representative examples):
  - Memory context realloc operations
  - Generic memory management routines

## Notes and Other Information
- Explicitly does not support actual reallocation to different sizes
- Uses test-and-elog instead of Assert for block validation due to high likelihood of errors
- Includes VALGRIND memory access control for debugging support
- The design philosophy prioritizes slab allocator efficiency over reallocation flexibility
- Returns NULL after error (though execution stops at elog(ERROR))

## Simplified Source
```c
void *
SlabRealloc(void *pointer, Size size, int flags)
{
    MemoryChunk *chunk = PointerGetMemoryChunk(pointer);
    SlabBlock *block = MemoryChunkGetBlock(chunk);

    // Validate the block belongs to a slab context
    if (!SlabBlockIsValid(block))
        elog(ERROR, "could not find block containing chunk %p", chunk);

    SlabContext *slab = block->slab;

    // Allow realloc only if size matches exactly (slab uses fixed-size chunks)
    if (size == slab->chunkSize)
        return pointer;

    // Slab allocator cannot change chunk sizes
    elog(ERROR, "slab allocator does not support realloc()");
    return NULL;
}
```