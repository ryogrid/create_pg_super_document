# RT_COPY_ARRAYS_AND_DELETE

## Location
[src/include/lib/radixtree.h:2301-2335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L2301-L2335)

## Overview
A macro that defines a helper function name for copying array elements from source to destination arrays while skipping a specific element to be deleted.

## Definition
```c
#define RT_COPY_ARRAYS_AND_DELETE RT_MAKE_NAME(copy_arrays_and_delete)
```

The actual function implementation:
```c
static inline void
RT_COPY_ARRAYS_AND_DELETE(uint8 *dst_chunks, RT_PTR_ALLOC * dst_children,
                          uint8 *src_chunks, RT_PTR_ALLOC * src_children,
                          int count, int deletepos)
```

## Detailed Description
This macro creates a template function name for copying elements between parallel arrays while excluding a specific element (effectively deleting it during the copy operation). The function is used when shrinking radix tree nodes, where elements need to be copied from a larger node type to a smaller one while omitting one element.

The implementation uses a branch-free computation technique to efficiently skip the deleted element index without conditional statements in the inner loop, improving performance.

## Parameters / Member Variables
- `dst_chunks`: Destination array for chunk values (keys)
- `dst_children`: Destination array for child pointers
- `src_chunks`: Source array of chunk values to copy from
- `src_children`: Source array of child pointers to copy from
- `count`: Total number of elements in the source arrays
- `deletepos`: Index position of the element to skip during copy (0-based)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for generating function names)
- Called from (representative examples):
  - [RT_SHRINK_NODE_16](RT_SHRINK_NODE_16.md) (src/include/lib/radixtree.h:2485)

## Notes and Other Information
- This is part of PostgreSQL's template-based radix tree implementation
- Uses branch-free computation: `sourceidx = i + (i >= deletepos)` to skip the deleted element
- Copies count-1 elements total (excluding the deleted one)
- Both chunk and children arrays are copied simultaneously to maintain correspondence
- Typically used during node shrinking operations when converting from larger to smaller node types