# RT_COPY_ARRAYS_FOR_INSERT

## Location
src/include/lib/radixtree.h: 1255 - 1271

## Overview
A macro that resolves to a static inline function used for copying arrays during node insertion operations in the radix tree data structure, creating space for a new element at a specified position.

## Definition


## Detailed Description
This function copies arrays from source to destination while leaving a gap at the specified insertion position. It implements a branch-free algorithm to efficiently copy array elements, automatically skipping the index where a new element will be inserted. The function copies both the chunk array (uint8 values representing key fragments) and the corresponding children pointer array in parallel, maintaining the relationship between keys and their child nodes.

## Parameters / Member Variables
- : Destination array for chunk values (key fragments)
- : Destination array for child node pointers
- : Source array for chunk values
- : Source array for child node pointers  
- : Number of elements to copy from the source arrays
- : Position where new element will be inserted (creates gap at this index)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for name generation)
- Called from (representative examples):
  - [RT_GROW_NODE_16](RT_GROW_NODE_16.md) (at src/include/lib/radixtree.h:1394)
  - [RT_GROW_NODE_4](RT_GROW_NODE_4.md) (at src/include/lib/radixtree.h:1495)

## Notes and Other Information
The function uses a clever branch-free computation  to skip the insertion position without conditional branching, improving performance. This is a key utility function for radix tree node growth operations when transitioning from smaller to larger node types (e.g., node4 to node16, node16 to node48).