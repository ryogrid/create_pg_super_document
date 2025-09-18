# RT_ALLOC_NODE

## Location
src/include/lib/radixtree.h: 834 - 896

## Overview
A macro that expands to a function allocating and initializing a new radix tree node of the specified kind and size class.

## Definition
```c
#define RT_ALLOC_NODE RT_MAKE_NAME(alloc_node)

static inline RT_CHILD_PTR
RT_ALLOC_NODE(RT_RADIX_TREE * tree, const uint8 kind, const RT_SIZE_CLASS size_class)
{
    RT_CHILD_PTR allocnode;
    RT_NODE    *node;
    size_t      allocsize;

    allocsize = RT_SIZE_CLASS_INFO[size_class].allocsize;

#ifdef RT_SHMEM
    allocnode.alloc = dsa_allocate(tree->dsa, allocsize);
#else
    allocnode.alloc = (RT_PTR_ALLOC) MemoryContextAlloc(tree->node_slabs[size_class], allocsize);
#endif

    RT_PTR_SET_LOCAL(tree, &allocnode);
    node = allocnode.local;

    /* initialize contents based on node kind */
    switch (kind)
    {
        case RT_NODE_KIND_4:
            memset(node, 0, offsetof(RT_NODE_4, children));
            break;
        case RT_NODE_KIND_16:
            memset(node, 0, offsetof(RT_NODE_16, children));
            break;
        case RT_NODE_KIND_48:
            {
                RT_NODE_48 *n48 = (RT_NODE_48 *) node;
                memset(n48, 0, offsetof(RT_NODE_48, slot_idxs));
                memset(n48->slot_idxs, RT_INVALID_SLOT_IDX, sizeof(n48->slot_idxs));
                break;
            }
        case RT_NODE_KIND_256:
            memset(node, 0, offsetof(RT_NODE_256, children));
            break;
        default:
            pg_unreachable();
    }

    node->kind = kind;
    node->fanout = RT_SIZE_CLASS_INFO[size_class].fanout;

    return allocnode;
}
```

## Detailed Description
This function allocates memory for a new radix tree node and initializes it according to the specified node kind. The function handles both shared memory (RT_SHMEM) and local memory contexts for allocation, making it suitable for different PostgreSQL usage scenarios.

The allocation process:
1. Determines allocation size from RT_SIZE_CLASS_INFO lookup table
2. Allocates memory using either DSA (dynamic shared memory) or MemoryContext
3. Sets up local pointer access for the allocated node
4. Initializes node contents based on the node kind:
   - NODE_KIND_4/16/256: Zero-initializes up to the children array
   - NODE_KIND_48: Special handling to initialize slot index array with invalid values
5. Sets node metadata (kind and fanout)
6. Returns the allocated node pointer structure

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure managing allocation contexts
- `kind`: Node type (RT_NODE_KIND_4, RT_NODE_KIND_16, RT_NODE_KIND_48, or RT_NODE_KIND_256)
- `size_class`: Size class determining allocation size and fanout limits

## Dependencies
- Functions called/Symbols referenced:
  - RT_SIZE_CLASS_INFO (size and fanout lookup table)
  - dsa_allocate (dynamic shared memory allocation, if RT_SHMEM)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (local memory allocation, if not RT_SHMEM)
  - [RT_PTR_SET_LOCAL](RT_PTR_SET_LOCAL.md) (pointer setup for shared memory access)
  - RT_INVALID_SLOT_IDX (constant for node48 slot initialization)
  - RT_MAKE_NAME (macro name generation)
- Called from (representative examples):
  - RT_GROW_NODE_* functions (when expanding smaller nodes)
  - [RT_EXTEND_UP](RT_EXTEND_UP.md) (when growing tree upward)
  - [RT_EXTEND_DOWN](RT_EXTEND_DOWN.md) (when growing tree downward)
  - RT_CREATE (when creating initial root node)
  - RT_SHRINK_NODE_* functions (when contracting larger nodes)

## Notes and Other Information
- Returns RT_CHILD_PTR structure containing both allocation handle and local pointer
- Supports both shared memory (multi-process) and local memory (single-process) allocation modes
- [Node](../N/Node.md) initialization is kind-specific to ensure proper empty state for each node type
- The fanout field may overflow to zero for NODE_KIND_256, which is acceptable since that node type doesn't introspect this value
- Part of PostgreSQL's generic radix tree implementation for high-performance key-value storage
- Memory allocation failures will be handled by the underlying PostgreSQL memory management system