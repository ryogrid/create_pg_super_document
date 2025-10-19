# RT_GET_SLOT_RECURSIVE

## Location
[src/include/lib/radixtree.h:1663-1704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1663-L1704)

## Overview
RT_GET_SLOT_RECURSIVE is the main workhorse function for RT_SET that recursively traverses the radix tree to find or create a slot for a given key.

## Definition
```c
static RT_PTR_ALLOC *
RT_GET_SLOT_RECURSIVE(RT_RADIX_TREE * tree, RT_PTR_ALLOC * parent_slot, uint64 key, int shift, bool *found)
```

## Detailed Description
This function recursively traverses the radix tree to locate the appropriate slot for a given key. At each level, it extracts the relevant chunk from the key and searches for it in the current node. If the chunk is not found, it creates a new slot and may extend the tree downward if more levels are needed. If the chunk is found and we are at shift level 0 (leaf level), it returns the existing slot. If found but not at the leaf level, it continues recursively to the next level down. The function maintains the tree structure while navigating and ensures proper slot allocation for new entries.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure being traversed
- `parent_slot`: Address of the child pointer in the parent node, needed for potential node growth during insertion
- `key`: The 64-bit key being searched or inserted
- `shift`: Current shift level indicating the tree depth
- `found`: Output parameter set to true if an existing entry was found, false if a new slot was created

## Dependencies
- Functions called/Symbols referenced:
  - RT_GET_KEY_CHUNK
  - [RT_PTR_SET_LOCAL](RT_PTR_SET_LOCAL.md)
  - [RT_NODE_SEARCH](RT_NODE_SEARCH.md)
  - [RT_NODE_INSERT](RT_NODE_INSERT.md)
  - [RT_EXTEND_DOWN](RT_EXTEND_DOWN.md)
  - RT_SPAN
- Called from (representative examples):
  - [RT_SET](RT_SET.md)
  - [RT_GET_SLOT_RECURSIVE](RT_GET_SLOT_RECURSIVE.md) (recursive self-call)

## Notes and Other Information
- This is the core recursive function that implements the tree traversal logic for insertions
- Handles both finding existing slots and creating new ones as needed
- The parent_slot parameter is crucial for handling node growth during insertion operations
- Uses tail recursion for efficient traversal down the tree levels
- The found parameter allows callers to distinguish between insertions and updates
- Critical for maintaining radix tree structure integrity during modifications

## Simplified Source

```c
static RT_PTR_ALLOC *
RT_GET_SLOT_RECURSIVE(RT_RADIX_TREE *tree, RT_PTR_ALLOC *parent_slot, uint64 key, int shift, bool *found)
{
    RT_PTR_ALLOC *slot;
    RT_CHILD_PTR node;
    uint8 chunk = RT_GET_KEY_CHUNK(key, shift);

    // Get the current node and search for the key chunk
    node.alloc = *parent_slot;
    RT_PTR_SET_LOCAL(tree, &node);
    slot = RT_NODE_SEARCH(node.local, chunk);

    if (slot == NULL) {
        // Chunk not found - create new slot
        *found = false;
        slot = RT_NODE_INSERT(tree, parent_slot, node, chunk);

        if (shift == 0)
            return slot;  // At leaf level
        else
            return RT_EXTEND_DOWN(tree, slot, key, shift);  // Need more levels
    } else {
        // Chunk found - check if we're at leaf level
        if (shift == 0) {
            *found = true;
            return slot;  // Found existing entry
        } else {
            // Continue recursively to next level
            return RT_GET_SLOT_RECURSIVE(tree, slot, key, shift - RT_SPAN, found);
        }
    }
}
```