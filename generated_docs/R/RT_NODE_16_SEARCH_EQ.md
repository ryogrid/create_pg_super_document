# RT_NODE_16_SEARCH_EQ

## Location
[src/include/lib/radixtree.h:983-1041](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L983-L1041)

## Overview
RT_NODE_16_SEARCH_EQ is a macro that resolves to a function for searching a node16 structure to find a child pointer corresponding to a specific key chunk.

## Definition
```c
#define RT_NODE_16_SEARCH_EQ RT_MAKE_NAME(node_16_search_eq)

// The actual function signature:
static inline RT_PTR_ALLOC * RT_NODE_16_SEARCH_EQ(RT_NODE_16 * node, uint8 chunk)
```

## Detailed Description
RT_NODE_16_SEARCH_EQ is part of PostgreSQL's templated radix tree implementation that provides optimized searching within node16 structures. This function implements both SIMD-accelerated and scalar search algorithms to find a child pointer corresponding to a given key chunk. The SIMD version uses vector operations to compare the search key against multiple stored chunks simultaneously, while the scalar version provides a fallback for platforms without SIMD support. The function includes assertion checking to verify that both implementations produce identical results when both are available.

## Parameters / Member Variables
- `node`: Pointer to the RT_NODE_16 structure to search within
- `chunk`: 8-bit key chunk value to search for in the node's chunk array

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for name generation)
  - [vector8_broadcast](../v/vector8_broadcast.md) (SIMD function for replicating values)
  - [vector8_load](../v/vector8_load.md) (SIMD function for loading vector data)
  - [vector8_eq](../v/vector8_eq.md) (SIMD function for parallel comparison)
  - [vector8_highbit_mask](../v/vector8_highbit_mask.md) (SIMD function for extracting comparison results)
  - [pg_rightmost_one_pos32](../p/pg_rightmost_one_pos32.md) (utility function for bit position finding)
  - Assert (debug assertion macro)
- Called from (representative examples):
  - [RT_NODE_SEARCH](RT_NODE_SEARCH.md) (general node search dispatcher)

## Notes and Other Information
- Implements dual code paths: optimized SIMD version and scalar fallback for compatibility
- SIMD implementation processes chunks in parallel using 8-byte vectors
- Scalar implementation uses simple linear search through the chunk array
- Both implementations are compiled when USE_ASSERT_CHECKING is enabled to verify correctness
- Returns pointer to the child slot if found, NULL if the chunk is not present in the node
- Function is marked inline for performance optimization since it's in the critical search path
- The SIMD version masks off invalid entries beyond the actual count to prevent false matches
- Uses bitfield operations to convert SIMD comparison results to array indices efficiently

## Simplified Source

```c
static inline RT_PTR_ALLOC *
RT_NODE_16_SEARCH_EQ(RT_NODE_16 *node, uint8 chunk)
{
    int count = node->base.count;

#ifdef USE_NO_SIMD
    // Scalar version: linear search through chunks
    for (int i = 0; i < count; i++) {
        if (node->chunks[i] == chunk) {
            return &node->children[i];
        }
    }
    return NULL;
#else
    // SIMD version: parallel search using vectors
    Vector8 spread_chunk = vector8_broadcast(chunk);
    Vector8 haystack1, haystack2, cmp1, cmp2;
    uint32 bitfield;

    // Load chunks and compare in parallel
    vector8_load(&haystack1, &node->chunks[0]);
    vector8_load(&haystack2, &node->chunks[sizeof(Vector8)]);
    cmp1 = vector8_eq(spread_chunk, haystack1);
    cmp2 = vector8_eq(spread_chunk, haystack2);

    // Convert comparison results to bitfield
    bitfield = vector8_highbit_mask(cmp1) | (vector8_highbit_mask(cmp2) << sizeof(Vector8));

    // Mask off entries beyond count and find match
    bitfield &= ((UINT64CONST(1) << count) - 1);
    if (bitfield)
        return &node->children[pg_rightmost_one_pos32(bitfield)];

    return NULL;
#endif
}
```