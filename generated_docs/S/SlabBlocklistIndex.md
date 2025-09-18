# SlabBlocklistIndex

## Location
[src/backend/utils/mmgr/slab.c:211-250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/slab.c#L211-L250)

## Overview
SlabBlocklistIndex is an internal function that determines which blocklist index a memory block should be assigned to based on the number of free chunks it contains.

## Definition


## Detailed Description
This function implements a critical part of the slab allocator's block management strategy. It calculates the appropriate blocklist index for a block based on its number of free chunks, using an efficient bit-shifting algorithm. The function uses two's complement arithmetic tricks to ensure that blocks with 0 free chunks are always assigned to index 0, while blocks with any number of free chunks (1 or more) are assigned to indices 1 through SLAB_BLOCKLIST_COUNT-1.

The algorithm exploits the property that 0 and -0 are identical in two's complement representation. By negating the free chunk count, bit-shifting right by blocklist_shift positions, and then negating again, it efficiently maps the free chunk count to the appropriate blocklist index.

## Parameters / Member Variables
- : Pointer to the SlabContext containing configuration information including blocklist_shift
- : Number of free chunks in the block (must be >= 0 and <= slab->chunksPerBlock)

## Dependencies
- Functions called/Symbols referenced:
  - [SlabContext](SlabContext.md) (struct type)
  - SLAB_BLOCKLIST_COUNT (constant)
- Called from (representative examples):
  - [SlabAllocFromNewBlock](SlabAllocFromNewBlock.md)
  - [SlabAlloc](SlabAlloc.md)
  - [SlabFree](SlabFree.md)
  - [SlabCheck](SlabCheck.md)

## Notes and Other Information
- This is a static inline function for performance optimization
- The function includes assertions to validate input parameters and verify correct index calculation
- The bit-shifting approach provides O(1) time complexity for index calculation
- The blocklist_shift value in SlabContext determines the granularity of the index mapping
- Index 0 is reserved exclusively for completely full blocks (0 free chunks)