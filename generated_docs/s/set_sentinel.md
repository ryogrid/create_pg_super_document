# set_sentinel

## Location
[src/include/utils/memdebug.h:51-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/memdebug.h#L51-L60)

## Overview
A memory debugging function that places a sentinel byte at a specific offset from a base memory address to detect buffer overruns and memory corruption.

## Definition


## Detailed Description
The `set_sentinel` function is a crucial component of PostgreSQL's memory debugging infrastructure. It places a sentinel byte (0x7E) at a calculated position relative to a base memory address. This sentinel acts as a canary value that can be checked later to detect buffer overruns, memory corruption, or other memory-related bugs. The function integrates with Valgrind to properly mark the sentinel byte location, first making it writable, then setting the value, and finally marking it as inaccessible to normal code execution.

The sentinel is typically placed just after allocated memory blocks, creating a guard zone that will be corrupted if code writes beyond the intended boundaries of the allocated memory.

## Parameters / Member Variables
- `base`: Base pointer to the memory region where the sentinel will be placed
- `offset`: Offset in bytes from the base pointer where the sentinel byte should be written

## Dependencies
- Functions called/Symbols referenced:
  - VALGRIND_MAKE_MEM_UNDEFINED
  - VALGRIND_MAKE_MEM_NOACCESS

- Called from (representative examples):
  - [AllocSetAllocLarge](../A/AllocSetAllocLarge.md) (src/backend/utils/mmgr/aset.c:733)
  - [AllocSetAlloc](../A/AllocSetAlloc.md) (src/backend/utils/mmgr/aset.c:1020)
  - [AllocSetRealloc](../A/AllocSetRealloc.md) (src/backend/utils/mmgr/aset.c:1280)
  - [BumpAllocLarge](../B/BumpAllocLarge.md) (src/backend/utils/mmgr/bump.c:338)
  - [GenerationAllocLarge](../G/GenerationAllocLarge.md) (src/backend/utils/mmgr/generation.c:388)
  - [SlabAllocSetupNewChunk](../S/SlabAllocSetupNewChunk.md) (src/backend/utils/mmgr/slab.c:519)

## Notes and Other Information
- Uses the byte value 0x7E as the sentinel value, which is easily distinguishable from common patterns
- The sentinel byte is placed in memory that should not be accessed by normal program execution
- Valgrind integration ensures that any attempt to read or write the sentinel location (other than through debugging functions) will be detected
- This function works in conjunction with `sentinel_ok` to provide comprehensive buffer overrun detection
- Primarily used by PostgreSQL's memory context allocators to add debugging capabilities to memory allocations