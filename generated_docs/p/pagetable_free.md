# pagetable_free

## Location
[src/backend/nodes/tidbitmap.c:1522-1541](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L1522-L1541)

## Overview
A callback function for freeing hash table elements in TID bitmaps, handling both regular memory and dynamic shared memory cleanup.

## Definition


## Detailed Description
This function serves as a memory deallocation callback for pagetable hash structures in TID bitmaps. It implements a dual deallocation strategy based on whether DSA (Dynamic Shared Area) is being used. For regular memory contexts, it directly frees the provided pointer using pfree. For DSA-based allocations, it frees the old pagetable reference (dsapagetableold) that was saved during the previous allocation, then resets the old pointer to invalid. This approach ensures proper cleanup of both current and previous allocations in shared memory scenarios.

## Parameters / Member Variables
- `pagetable`: Pointer to the hash table structure from which memory is being freed
- `pointer`: Pointer to the memory location to be freed (used only in non-DSA cases)

## Dependencies
- Functions called/Symbols referenced:
  - [TIDBitmap](../T/TIDBitmap.md) (struct type)
  - DsaPointerIsValid (function)
  - [dsa_free](../d/dsa_free.md) (function)
  - InvalidDsaPointer (constant)
  - [pfree](pfree.md) (function)
- Called from (representative examples):
  - Used as callback in hash table operations (not directly referenced)

## Notes and Other Information
- Static inline function for performance in memory deallocation hot paths
- Complements pagetable_allocate by handling the corresponding cleanup
- In DSA mode, frees the old allocation rather than the current pointer
- Properly manages DSA pointer validity and invalidation
- Critical for preventing memory leaks in both regular and parallel TID bitmap operations