# RT_EXTEND_DOWN

## Location
[src/include/lib/radixtree.h:1616-1662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1616-L1662)

## Overview
RT_EXTEND_DOWN is a static inline function that inserts a chain of nodes downward in the radix tree until reaching the lowest level, returning the address of a slot to be filled by the caller.

## Definition
```c
static pg_noinline RT_PTR_ALLOC *
RT_EXTEND_DOWN(RT_RADIX_TREE * tree, RT_PTR_ALLOC * parent_slot, uint64 key, int shift)
```

## Detailed Description
This function creates a chain of new nodes extending downward from a given parent slot to accommodate a key at a specific shift level. It allocates RT_NODE_KIND_4 nodes and links them together, with each node containing exactly one child pointer. The function continues creating nodes until it reaches shift level 0 (the leaf level), where it reserves a slot for the actual value. This is typically used when inserting a new key-value pair that requires extending the tree structure downward.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure being modified
- `parent_slot`: Address of the slot in the parent node where the first new node will be linked
- `key`: The 64-bit key being inserted, used to determine chunk values at each level
- `shift`: The current shift level, indicating how many levels down to extend

## Dependencies
- Functions called/Symbols referenced:
  - [RT_ALLOC_NODE](RT_ALLOC_NODE.md)
  - RT_GET_KEY_CHUNK
  - RT_SPAN
  - RT_NODE_KIND_4
  - RT_CLASS_4
- Called from (representative examples):
  - [RT_GET_SLOT_RECURSIVE](RT_GET_SLOT_RECURSIVE.md)
  - [RT_SET](RT_SET.md) (during tree extension operations)

## Notes and Other Information
- The function is marked as pg_noinline, indicating it is not meant to be inlined for performance reasons
- Creates a chain of RT_NODE_4 nodes, each with exactly one child until reaching the leaf level
- The function uses open-coded insertion for speed optimization
- Returns a pointer to the slot where the actual value should be stored
- Critical for radix tree expansion when new keys require deeper tree structures

## Simplified Source

```c
static pg_noinline RT_PTR_ALLOC *
RT_EXTEND_DOWN(RT_RADIX_TREE *tree, RT_PTR_ALLOC *parent_slot, uint64 key, int shift)
{
    RT_CHILD_PTR node, child;
    RT_NODE_4 *n4;

    // Create first node and link it to parent slot
    child = RT_ALLOC_NODE(tree, RT_NODE_KIND_4, RT_CLASS_4);
    *parent_slot = child.alloc;

    node = child;
    shift -= RT_SPAN;

    // Create chain of intermediate nodes
    while (shift > 0) {
        child = RT_ALLOC_NODE(tree, RT_NODE_KIND_4, RT_CLASS_4);

        // Set up current node with one child
        n4 = (RT_NODE_4 *) node.local;
        n4->base.count = 1;
        n4->chunks[0] = RT_GET_KEY_CHUNK(key, shift);
        n4->children[0] = child.alloc;

        node = child;
        shift -= RT_SPAN;
    }

    // Set up leaf node for value slot
    n4 = (RT_NODE_4 *) node.local;
    n4->chunks[0] = RT_GET_KEY_CHUNK(key, 0);
    n4->base.count = 1;

    return &n4->children[0];
}
```