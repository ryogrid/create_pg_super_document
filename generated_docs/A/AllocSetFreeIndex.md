# AllocSetFreeIndex

## Location
[src/backend/utils/mmgr/aset.c:277-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/aset.c#L277-L346)

## Overview
Computes which freelist index a memory allocation of a given size belongs to in PostgreSQL's allocation set memory context.

## Definition

```c
static inline int
AllocSetFreeIndex(Size size)
```
## Detailed Description
AllocSetFreeIndex is a performance-critical inline function that determines the appropriate freelist index for a memory chunk based on its size. The function implements a logarithmic mapping where larger sizes map to higher indices, enabling efficient segregation of free chunks by size classes.

The algorithm computes ceil(log2(size >> ALLOC_MINBITS)) for sizes larger than the minimum allocation size. For optimal performance on platforms without intrinsic bit scan support, it includes hand-optimized bit manipulation code that unrolls loops and handles only the necessary bits for the expected size range.

The function assumes that the caller has already verified that size <= ALLOC_CHUNK_LIMIT, making it safe to use optimized 16-bit arithmetic operations.

## Parameters / Member Variables
- `size`: The size in bytes of the memory allocation for which to compute the freelist index
## Dependencies
- Functions called/Symbols referenced:
  - [pg_leftmost_one_pos32](../p/pg_leftmost_one_pos32.md) (on platforms with HAVE_BITSCAN_REVERSE)
  - pg_leftmost_one_pos (lookup table for bit position)
  - ALLOC_MINBITS (minimum allocation size bits)
  - ALLOC_CHUNK_LIMIT (maximum chunk size limit)
  - ALLOCSET_NUM_FREELISTS (total number of freelists)
  - StaticAssertDecl (compile-time assertion)

- Called from (representative examples):
  - [AllocSetAllocFromNewBlock](AllocSetAllocFromNewBlock.md)
  - [AllocSetAlloc](AllocSetAlloc.md)

## Notes and Other Information
- This is a static inline function optimized for high performance since it's called frequently during memory allocation
- Contains platform-specific optimizations using HAVE_BITSCAN_REVERSE for systems with hardware bit scan support  
- Includes hand-optimized fallback code for systems without intrinsic bit manipulation functions
- Uses compile-time assertions to ensure ALLOC_CHUNK_LIMIT fits in expected size constraints
- Returns index 0 for sizes <= (1 << ALLOC_MINBITS), effectively handling small allocations in a single freelist
- The logarithmic indexing scheme provides good distribution of chunk sizes across freelists while maintaining fast lookup performance