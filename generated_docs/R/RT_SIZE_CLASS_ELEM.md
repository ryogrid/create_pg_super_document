# RT_SIZE_CLASS_ELEM

## Location
[src/include/lib/radixtree.h:642-647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L642-L647)

## Overview
RT_SIZE_CLASS_ELEM is a macro that generates a type name for a structure containing metadata about different node size classes in PostgreSQL's adaptive radix tree implementation.

## Definition

```c
typedef struct RT_SIZE_CLASS_ELEM
{
	const char *name;
	int			fanout;
	size_t		allocsize;
}			RT_SIZE_CLASS_ELEM;
```
## Detailed Description
RT_SIZE_CLASS_ELEM is part of PostgreSQL's adaptive radix tree (ART) implementation and defines a structure that holds metadata for each of the different node size classes. The adaptive radix tree uses multiple node types (4, 16, 48, and 256 slots) to optimize memory usage and performance based on the number of children a node needs to store.

This structure serves as a lookup table entry that provides essential information about each size class, including its display name, maximum number of children (fanout), and the total memory allocation size needed. This metadata is used by the memory allocation system to determine how much space to allocate for nodes of each type and for debugging/statistics purposes.

The size class system is a key innovation that decouples the concept of node type from size class, allowing for variable-capacity nodes within each type (particularly for the 16-slot and 48-slot variants which have low and high capacity versions).

## Parameters / Member Variables
- : A string identifier for the size class, used for debugging and statistics display
- : The maximum number of child slots this size class can accommodate
- : The total number of bytes to allocate for a node of this size class

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - RT_MAKE_PREFIX
  - RT_PREFIX
- Called from (representative examples):
  - RT_CREATE
  - [RT_STATS](RT_STATS.md)
  - RT_SIZE_CLASS_INFO (static array initialization)
  - [RT_ALLOC_NODE](RT_ALLOC_NODE.md) (for allocation size lookup)
  - [RT_FREE_NODE](RT_FREE_NODE.md) (for debugging statistics)

## Notes and Other Information
The RT_SIZE_CLASS_ELEM structure is used to populate the RT_SIZE_CLASS_INFO static array, which contains metadata for all five size classes:

1. **RT_CLASS_4**: Basic 4-slot node
2. **RT_CLASS_16_LO**: Low-capacity 16-slot node variant
3. **RT_CLASS_16_HI**: High-capacity 16-slot node variant  
4. **RT_CLASS_48**: 48-slot node
5. **RT_CLASS_256**: 256-slot node

The dual 16-slot classes (LO/HI) and the variable fanout system allow the radix tree to optimize memory usage for different deployment scenarios, particularly when shared memory (RT_SHMEM) is involved where DSA (Dynamic Shared Area) size classes need to be considered.

This metadata-driven approach enables the adaptive radix tree to make informed decisions about node growth and shrinkage, ensuring optimal memory utilization while maintaining performance characteristics.