# tuplesort_readtup_alloc

## Location
[src/backend/utils/sort/tuplesort.c:2921-2954](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2921-L2954)

## Overview
Allocates memory for tuple storage during tuple reading operations, using either a slab allocator for efficiency or standard palloc() for larger tuples.

## Definition

```c
void *
tuplesort_readtup_alloc(Tuplesortstate *state, Size tuplen)
```
## Detailed Description
The `tuplesort_readtup_alloc` function provides memory allocation specifically designed for tuple reading operations within the READTUP() routines. It implements a two-tier allocation strategy: for tuples that fit within the predefined slab slot size (`SLAB_SLOT_SIZE`), it uses a fast slab allocator that pre-allocates memory slots to avoid repeated malloc/free operations. For larger tuples that exceed the slab slot size, it falls back to the standard PostgreSQL memory allocation via `MemoryContextAlloc`.

This function is crucial for performance optimization during external merge sort operations, where many tuples need to be allocated and deallocated rapidly. The slab allocator reduces memory fragmentation and allocation overhead for the common case of reasonably-sized tuples.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate containing the slab allocator state and sort context
- `tuplen`: Size in bytes of the tuple memory to be allocated

## Dependencies
- Functions called/Symbols referenced:
  - [Tuplesortstate](../T/Tuplesortstate.md) (structure type)
  - SlabSlot (structure type for slab allocation slots)
  - SLAB_SLOT_SIZE (constant defining maximum slab slot size)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (PostgreSQL memory allocation function)
- Called from (representative examples):
  - [readtup_heap](../r/readtup_heap.md) (src/backend/utils/sort/tuplesortvariants.c:1182)
  - [readtup_cluster](../r/readtup_cluster.md) (src/backend/utils/sort/tuplesortvariants.c:1376)
  - [readtup_index](../r/readtup_index.md) (src/backend/utils/sort/tuplesortvariants.c:1693)
  - [readtup_index_brin](../r/readtup_index_brin.md) (src/backend/utils/sort/tuplesortvariants.c:1766)
  - [readtup_datum](../r/readtup_datum.md) (src/backend/utils/sort/tuplesortvariants.c:1880)

## Notes and Other Information
- This is a public function (non-static), accessible from other compilation units
- The function assumes that enough slab slots have been pre-allocated and should never run out (enforced by Assert)
- Memory allocated through this function is automatically cleaned up when the sort context is destroyed
- The slab allocator uses a simple linked list of free slots for fast allocation/deallocation
- Larger tuples that don't fit in slab slots are allocated directly from the sort's memory context
- Used exclusively during the merge phase of external sorting when reading tuples from temporary storage

## Simplified Source

```c
void *
tuplesort_readtup_alloc(Tuplesortstate *state, Size tuplen)
{
    SlabSlot *buf;

    Assert(state->slabFreeHead);

    // Use standard allocation for large tuples or if no slab slots available
    if (tuplen > SLAB_SLOT_SIZE || !state->slabFreeHead) {
        return MemoryContextAlloc(state->base.sortcontext, tuplen);
    } else {
        // Reuse next available slab slot for small tuples
        buf = state->slabFreeHead;
        state->slabFreeHead = buf->nextfree;
        return buf;
    }
}
```