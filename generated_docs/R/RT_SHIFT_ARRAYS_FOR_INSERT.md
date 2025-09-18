# RT_SHIFT_ARRAYS_FOR_INSERT

## Location
src/include/lib/radixtree.h: 1233 - 1254

## Overview
RT_SHIFT_ARRAYS_FOR_INSERT is a macro that expands to a function that shifts elements in both chunks and children arrays to make room for inserting a new element at a specific position.

## Definition
```c
#define RT_SHIFT_ARRAYS_FOR_INSERT RT_MAKE_NAME(shift_arrays_for_insert)

static inline void
RT_SHIFT_ARRAYS_FOR_INSERT(uint8 *chunks, RT_PTR_ALLOC *children, int count, int insertpos)
```

## Detailed Description
RT_SHIFT_ARRAYS_FOR_INSERT implements an array element shifting operation that moves existing elements one position to the right to create space for inserting a new element at a specified position. This function is essential for maintaining sorted order in node arrays when inserting new chunks.

The function operates on two parallel arrays simultaneously: the chunks array (containing key fragments) and the children array (containing pointers to child nodes or values). It performs a backwards iteration from the last element down to the insertion position, moving each element one position to the right.

The implementation uses a simple loop instead of memmove() for performance reasons on small inputs, as radix tree nodes typically contain small numbers of elements (4 or 16). The function includes a GCC-specific workaround for a compiler optimization bug that could affect the shifting operation.

After this function completes, the caller can safely insert the new chunk and child at the insertpos index without overwriting existing data.

## Parameters / Member Variables
- `chunks`: Pointer to the array of uint8 key chunks to shift
- `children`: Pointer to the array of RT_PTR_ALLOC child pointers to shift  
- `count`: Number of currently used elements in the arrays
- `insertpos`: Index position where the new element will be inserted (0-based)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro expansion)
  - GCC inline assembly (compiler-specific workaround)
- Called from (representative examples):
  - [RT_ADD_CHILD_16](RT_ADD_CHILD_16.md)
  - [RT_ADD_CHILD_4](RT_ADD_CHILD_4.md)

## Notes and Other Information
- Modifies both input arrays in-place, shifting elements from insertpos to count-1
- Assumes sufficient space exists in the arrays for the additional element
- Uses a backwards loop to avoid overwriting data during the shift operation
- Optimized for small array sizes typical in radix tree nodes
- Contains a GCC-specific inline assembly workaround for compiler bug #101481
- The function does not actually insert the new element - it only creates space for it
- Part of the array manipulation utilities within the radixtree template system
- Critical for maintaining sorted order when adding elements to node arrays