# RT_NODE_4_GET_INSERTPOS

## Location
[src/include/lib/radixtree.h:1142-1159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1142-L1159)

## Overview
RT_NODE_4_GET_INSERTPOS is a macro that expands to a function that finds the correct insertion position for a new chunk in a node-4's sorted arrays.

## Definition
```c
#define RT_NODE_4_GET_INSERTPOS RT_MAKE_NAME(node_4_get_insertpos)

static inline int
RT_NODE_4_GET_INSERTPOS(RT_NODE_4 *node, uint8 chunk, int count)
```

## Detailed Description
RT_NODE_4_GET_INSERTPOS implements a linear search algorithm to find the appropriate insertion position for a new chunk value in a node-4's chunks array. Node-4 stores its chunks in sorted order to enable efficient searching and binary operations. The function iterates through the existing chunks and returns the index where the new chunk should be inserted to maintain sorted order.

The function performs a simple linear scan comparing the new chunk value against existing chunks. When it finds the first existing chunk that is greater than or equal to the new chunk, it returns that index as the insertion position. If no such chunk is found (i.e., the new chunk is larger than all existing chunks), it returns the count, indicating the chunk should be appended to the end.

## Parameters / Member Variables
- `node`: Pointer to the RT_NODE_4 structure to find insertion position in
- `chunk`: 8-bit key fragment (byte) to find insertion position for
- `count`: Number of currently used slots in the node (must be <= RT_FANOUT_4)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro expansion)
- Called from (representative examples):
  - [RT_GROW_NODE_4](RT_GROW_NODE_4.md)
  - [RT_ADD_CHILD_4](RT_ADD_CHILD_4.md)

## Notes and Other Information
- Returns an integer index (0 to count) where the new chunk should be inserted
- Maintains sorted order of chunks in the node-4 arrays
- Uses linear search which is efficient for small arrays (node-4 has maximum 4 elements)
- The returned index can equal count if the new chunk should be appended at the end
- Part of the node-4 specific operations within the radixtree template system
- Critical for maintaining the sorted invariant required for efficient node-4 operations