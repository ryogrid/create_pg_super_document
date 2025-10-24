# randomize_mem

## Location
[src/backend/utils/mmgr/memdebug.c:75-93](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/memdebug.c#L75-L93)

## Overview
Fills a newly allocated memory region with pseudo-random data to help detect code that incorrectly depends on uninitialized memory contents.

## Definition
```c
void randomize_mem(char *ptr, size_t size)
```

## Detailed Description
The `randomize_mem` function fills a just-allocated piece of memory with "random" data to facilitate debugging of uninitialized memory usage. The randomization is not cryptographically secure but uses a repeating sequence with a prime length (251) to ensure that two allocations of the same size are likely to contain different initial data patterns.

This function is only compiled when `RANDOMIZE_ALLOCATED_MEMORY` is defined, which is a debugging feature that can be enabled in `pg_config_manual.h`. The configuration comment warns that this feature is "horrendously expensive" and should only be used for debugging purposes.

The function handles Valgrind integration properly by marking the memory region as undefined before writing to it (to avoid access errors) and then marking it as undefined again afterward. This ensures compatibility with memory debugging tools while still filling the memory with the desired pattern.

The pseudo-random pattern uses a static counter that cycles from 1 to 251, ensuring that consecutive allocations receive different bit patterns, which helps expose bugs where code assumes specific initial memory contents.

## Parameters / Member Variables
- `ptr`: Pointer to the memory region to randomize
- `size`: Size of the memory region in bytes

## Dependencies
- Functions called/Symbols referenced:
  - VALGRIND_MAKE_MEM_UNDEFINED (macro for Valgrind integration)
- Called from (representative examples):
  - [AllocSetAllocLarge](../A/AllocSetAllocLarge.md) (src/backend/utils/mmgr/aset.c:737)
  - [AllocSetAllocChunkFromBlock](../A/AllocSetAllocChunkFromBlock.md) (src/backend/utils/mmgr/aset.c:798)
  - [AllocSetAlloc](../A/AllocSetAlloc.md) (src/backend/utils/mmgr/aset.c:1024)
  - [AllocSetRealloc](../A/AllocSetRealloc.md) (src/backend/utils/mmgr/aset.c:1258, 1339)
  - [BumpAllocLarge](../B/BumpAllocLarge.md) (src/backend/utils/mmgr/bump.c:342)
  - [BumpAllocChunkFromBlock](../B/BumpAllocChunkFromBlock.md) (src/backend/utils/mmgr/bump.c:406)
  - [GenerationAllocLarge](../G/GenerationAllocLarge.md) (src/backend/utils/mmgr/generation.c:392)
  - [GenerationAllocChunkFromBlock](../G/GenerationAllocChunkFromBlock.md) (src/backend/utils/mmgr/generation.c:440)
  - [GenerationRealloc](../G/GenerationRealloc.md) (src/backend/utils/mmgr/generation.c:871)
  - [SlabAllocSetupNewChunk](../S/SlabAllocSetupNewChunk.md) (src/backend/utils/mmgr/slab.c:528)

## Notes and Other Information
- This function is conditionally compiled only when `RANDOMIZE_ALLOCATED_MEMORY` is defined
- The pseudo-random pattern uses a prime number (251) for the cycle length to improve distribution
- Maintains a static counter (`save_ctr`) that persists across function calls
- Integrates with Valgrind by properly marking memory regions as undefined
- Used exclusively for debugging purposes to catch uninitialized memory bugs
- Performance impact is significant and should not be used in production builds
- The function is called by all major memory allocators in PostgreSQL's memory management system

## Simplified Source

```c
void
randomize_mem(char *ptr, size_t size)
{
    static int save_ctr = 1;
    size_t remaining = size;
    int ctr;

    ctr = save_ctr;

    // Mark memory for Valgrind debugging
    VALGRIND_MAKE_MEM_UNDEFINED(ptr, size);

    // Fill memory with pseudo-random pattern
    while (remaining-- > 0)
    {
        *ptr++ = ctr;
        if (++ctr > 251)  // Use prime number for better distribution
            ctr = 1;
    }

    // Mark memory as undefined for Valgrind
    VALGRIND_MAKE_MEM_UNDEFINED(ptr - size, size);
    save_ctr = ctr;
}
```