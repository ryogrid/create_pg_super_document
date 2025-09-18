# RT_SHIFT_ARRAYS_AND_DELETE

## Location
[src/include/lib/radixtree.h:2279-2300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L2279-L2300)

## Overview
A macro that defines a helper function name for shifting array elements and deleting an element at a specific position in radix tree nodes.

## Definition
```c
#define RT_SHIFT_ARRAYS_AND_DELETE RT_MAKE_NAME(shift_arrays_and_delete)
```

The actual function implementation:
```c
static inline void
RT_SHIFT_ARRAYS_AND_DELETE(uint8 *chunks, RT_PTR_ALLOC * children, int count, int deletepos)
```

## Detailed Description
This macro creates a template function name for deleting an element from parallel arrays used in radix tree nodes. The function performs an in-place deletion by shifting all elements after the deletion position one position to the left, effectively overwriting the deleted element. The implementation uses a simple loop instead of memmove for performance optimization on small array sizes typical in radix tree nodes.

The function contains a GCC-specific workaround (inline assembly with empty string) to address GCC bug #101481 which can cause incorrect optimizations in certain scenarios.

## Parameters / Member Variables
- `chunks`: Pointer to the array of chunk values (keys) to be modified
- `children`: Pointer to the parallel array of child pointers to be modified  
- `count`: Total number of elements currently in the arrays
- `deletepos`: Index position of the element to be deleted (0-based)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for generating function names)
- Called from (representative examples):
  - [RT_REMOVE_CHILD_16](RT_REMOVE_CHILD_16.md) (src/include/lib/radixtree.h:2517)
  - [RT_REMOVE_CHILD_4](RT_REMOVE_CHILD_4.md) (src/include/lib/radixtree.h:2565)

## Notes and Other Information
- This is part of PostgreSQL's template-based radix tree implementation
- The function shifts elements left to fill the gap left by deletion
- Contains a compiler-specific workaround for GCC optimization bug
- Designed for small array sizes where simple loops outperform memmove
- Both chunk and children arrays are modified simultaneously to maintain correspondence