# RT_STATS

## Location
src/include/lib/radixtree.h: 2794 - 2820

## Overview
RT_STATS is a macro that generates the name for a debugging function that prints detailed statistics and diagnostic information about a radix tree data structure in PostgreSQL.

## Definition
```c
#define RT_STATS RT_MAKE_NAME(stats)
```

The actual function signature when expanded:
```c
RT_SCOPE void RT_STATS(RT_RADIX_TREE *tree)
```

## Detailed Description
RT_STATS provides comprehensive debugging and diagnostic output for radix tree structures, printing various statistics to stderr. The function is only available when RT_DEBUG is defined during compilation, making it a debugging-only feature.

The function outputs key tree metrics including the maximum key value, total number of keys stored, tree height calculated from the start shift, and detailed node count statistics broken down by size class (node types with different fanout capacities). For shared memory trees, it also prints the DSA handle information.

The statistics include counts for each node type (4-way, 16-way, 48-way, and 256-way nodes) as well as the total number of leaf nodes. This information is valuable for understanding tree structure, memory usage patterns, and performance characteristics during development and debugging.

## Parameters / Member Variables
- `tree`: Pointer to the RT_RADIX_TREE structure for which to print statistics

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for generating function names)
  - fprintf (standard C library function for formatted output)
  - RT_SIZE_CLASS_INFO (array containing node size class information)
  - UINT64_FORMAT (PostgreSQL macro for uint64 formatting)
  - DSA_POINTER_FORMAT (format macro for DSA pointers, used in shared memory builds)

- Called from (representative examples):
  - RT_HANDLE (radixtree handle operations)
  - Debugging and diagnostic code
  - Performance analysis routines
  - Test suites and benchmarking code

## Notes and Other Information
- Only compiled and available when RT_DEBUG is defined during compilation
- All output is directed to stderr for debugging purposes
- Provides detailed breakdown of node distribution across different size classes
- For shared memory trees, includes DSA handle information for tracking shared allocations
- Tree height is calculated as start_shift / RT_SPAN, representing the maximum depth
- [Node](../N/Node.md) counts are tracked per size class: n4, n16, n48, n256 corresponding to different fanout capacities
- Leaf count represents the total number of leaf nodes containing actual values
- Format strings use PostgreSQL-specific macros for cross-platform compatibility
- Part of the debugging infrastructure for PostgreSQL's generic radix tree template system