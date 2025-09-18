# RT_NODE_16

## Location
src/include/lib/radixtree.h: 528 - 536

## Overview
RT_NODE_16 is a macro that expands to generate the medium-capacity node type in the adaptive radix tree implementation, designed for nodes with up to 32 children and optimized with SIMD operations.

## Definition
```c
#define RT_NODE_16 RT_MAKE_NAME(node_16)
```

Expands to the node structure:
```c
typedef struct RT_NODE_16
{
    RT_NODE base;
    uint8 chunks[RT_FANOUT_16_MAX];
    RT_PTR_ALLOC children[FLEXIBLE_ARRAY_MEMBER];
} RT_NODE_16;
```

## Detailed Description
RT_NODE_16 represents a medium-capacity internal node type in PostgreSQL's adaptive radix tree (ART) implementation. It extends RT_NODE_4's design to handle higher fanouts (typically 9-32 children) while maintaining the parallel array structure of key chunks and child pointers. The actual expansion follows the pattern: `{prefix}_node_16`, where prefix is determined by RT_PREFIX.

This node type is optimized with SIMD (Single Instruction, Multiple Data) operations for efficient parallel searching through key chunks. When SIMD is available, it can search multiple chunks simultaneously, significantly improving lookup performance. The node uses RT_FANOUT_16_MAX (typically 32) as its maximum capacity, equal to two 128-bit SIMD registers regardless of SIMD availability.

## Parameters / Member Variables
- `base`: The RT_NODE base structure containing kind, fanout, and count metadata
- `chunks`: Array of key chunks (byte values) stored in sorted order, with size RT_FANOUT_16_MAX (typically 32)
- `children`: Flexible array of child pointers corresponding to the chunks at the same indices

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for name generation)
  - [RT_NODE](RT_NODE.md) (base node structure)
  - RT_FANOUT_16_MAX (maximum capacity constant, typically 32)
  - RT_PTR_ALLOC (child pointer type)
  - FLEXIBLE_ARRAY_MEMBER (variable-length array support)
- Called from (representative examples):
  - [RT_ALLOC_NODE](RT_ALLOC_NODE.md) (node allocation with RT_NODE_KIND_16)
  - [RT_NODE_16_SEARCH_EQ](RT_NODE_16_SEARCH_EQ.md) (SIMD-optimized child lookup)
  - [RT_NODE_16_GET_INSERTPOS](RT_NODE_16_GET_INSERTPOS.md) (finding insertion position)
  - [RT_ADD_CHILD_16](RT_ADD_CHILD_16.md) (adding new child entries)
  - [RT_GROW_NODE_16](RT_GROW_NODE_16.md) (converting to RT_NODE_48 when capacity exceeded)
  - [RT_SHRINK_NODE_16](RT_SHRINK_NODE_16.md) (converting to RT_NODE_4 when underutilized)
  - [RT_REMOVE_CHILD_16](RT_REMOVE_CHILD_16.md) (removing child entries)

## Notes and Other Information
- Uses SIMD operations for parallel chunk searching when available, with fallback to linear search
- Maintains parallel arrays with chunks and children at corresponding indices
- Both arrays are kept sorted by chunk value for consistent ordering
- Automatically grows to RT_NODE_48 when capacity is exceeded
- Can shrink to RT_NODE_4 when child count drops below threshold  
- RT_FANOUT_16_MAX is set to 32, equal to two 128-bit SIMD registers
- Provides a good balance between memory efficiency and search performance
- The SIMD optimization uses vector operations to compare multiple chunks simultaneously
- Supports multiple size classes (RT_FANOUT_16_LO and RT_FANOUT_16_HI) for fine-tuned memory management
- Part of the adaptive behavior that optimizes both memory usage and search performance based on fanout requirements