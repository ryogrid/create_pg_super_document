# RT_NODE_16_GET_INSERTPOS

## Location
[src/include/lib/radixtree.h:1160-1232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1160-L1232)

## Overview
RT_NODE_16_GET_INSERTPOS is a macro that expands to a function that finds the correct insertion position for a new chunk in a node-16's sorted arrays using optimized SIMD or linear search.

## Definition
```c
#define RT_NODE_16_GET_INSERTPOS RT_MAKE_NAME(node_16_get_insertpos)

static inline int
RT_NODE_16_GET_INSERTPOS(RT_NODE_16 *node, uint8 chunk)
```

## Detailed Description
RT_NODE_16_GET_INSERTPOS implements an optimized search algorithm to find the appropriate insertion position for a new chunk value in a node-16's chunks array. Like node-4, node-16 maintains its chunks in sorted order for efficient operations, but uses more sophisticated algorithms due to the larger array size (up to 16 elements).

The function first performs an optimization for appending elements by checking if the new chunk is larger than the last element. If so, it returns the count directly, avoiding further search.

For other cases, the function uses one of two implementations based on compilation flags:
1. **SIMD version** (default): Uses vectorized operations with Vector8 instructions to perform parallel comparisons. It employs vector8_min() and vector8_eq() operations to find the first position where an existing chunk is greater than or equal to the new chunk.
2. **Non-SIMD version** (fallback): Uses a simple linear search similar to node-4's implementation.

The SIMD implementation is more complex as it works around the lack of unsigned uint8 comparison instructions in SSE2, using vector8_min() to effectively perform >= comparisons.

## Parameters / Member Variables
- `node`: Pointer to the RT_NODE_16 structure to find insertion position in
- `chunk`: 8-bit key fragment (byte) to find insertion position for

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro expansion)
  - [vector8_broadcast](../v/vector8_broadcast.md) (SIMD version)
  - [vector8_load](../v/vector8_load.md) (SIMD version)
  - [vector8_min](../v/vector8_min.md) (SIMD version)
  - [vector8_eq](../v/vector8_eq.md) (SIMD version)
  - [vector8_highbit_mask](../v/vector8_highbit_mask.md) (SIMD version)
  - [pg_rightmost_one_pos32](../p/pg_rightmost_one_pos32.md) (SIMD version)
  - Assert
- Called from (representative examples):
  - [RT_GROW_NODE_16](RT_GROW_NODE_16.md)
  - [RT_ADD_CHILD_16](RT_ADD_CHILD_16.md)

## Notes and Other Information
- Returns an integer index (0 to count) where the new chunk should be inserted
- Optimized for the common case of appending ordered keys (fast path)
- Uses SIMD vectorization when available for improved performance on larger arrays
- Maintains sorted order of chunks in the node-16 arrays
- The SIMD version processes chunks in parallel using vector operations
- Contains assertions to verify consistency between SIMD and non-SIMD implementations when both are compiled
- Part of the node-16 specific operations within the radixtree template system

## Simplified Source

```c
static inline int
RT_NODE_16_GET_INSERTPOS(RT_NODE_16 *node, uint8 chunk)
{
    int count = node->base.count;

    // Optimization: check if appending (common case for ordered insertions)
    if (node->chunks[count - 1] < chunk)
        return count;

#ifdef USE_NO_SIMD
    // Scalar version: linear search for insertion position
    for (int index = 0; index < count; index++) {
        if (node->chunks[index] > chunk)
            return index;
    }
    return count;
#else
    // SIMD version: parallel search using vector operations
    Vector8 spread_chunk = vector8_broadcast(chunk);
    Vector8 haystack1, haystack2, min1, min2, cmp1, cmp2;
    uint32 bitfield;

    // Load chunks and find positions where chunk <= haystack (using min trick)
    vector8_load(&haystack1, &node->chunks[0]);
    vector8_load(&haystack2, &node->chunks[sizeof(Vector8)]);
    min1 = vector8_min(spread_chunk, haystack1);
    min2 = vector8_min(spread_chunk, haystack2);

    // Compare to find positions where chunk <= haystack[i] (meaning haystack[i] >= chunk)
    cmp1 = vector8_eq(spread_chunk, min1);
    cmp2 = vector8_eq(spread_chunk, min2);

    // Convert to bitfield and find first match
    bitfield = vector8_highbit_mask(cmp1) | (vector8_highbit_mask(cmp2) << sizeof(Vector8));
    return pg_rightmost_one_pos32(bitfield);
#endif
}
```