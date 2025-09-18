# intset_new_leaf_node

## Location
src/backend/lib/integerset.c: 331 - 349

## Overview
Allocates and initializes a new leaf node for the IntegerSet B-tree structure that stores compressed integer data.

## Definition
```c
static intset_leaf_node * intset_new_leaf_node(IntegerSet *intset)
```

## Detailed Description
The `intset_new_leaf_node` function creates a new leaf node for the IntegerSet's B-tree structure. Leaf nodes are the bottom level of the tree and contain the actual compressed integer data using Simple-8b encoding. The function allocates memory in the IntegerSet's designated memory context and updates the memory usage tracking. The new leaf node is initialized with default values and linked list pointers set to NULL.

## Parameters / Member Variables
- `intset`: Pointer to the IntegerSet structure that will contain this leaf node

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Memory allocation in specific context
  - `GetMemoryChunkSpace`: Function to get allocated memory size
  - [intset_leaf_node](intset_leaf_node.md): Structure type for leaf nodes
- Called from (representative examples):
  - [intset_flush_buffered_values](intset_flush_buffered_values.md): Used when creating new leaf nodes to store buffered values

## Notes and Other Information
- This is a static function, only accessible within the integerset.c file
- Memory is allocated in the IntegerSet's designated memory context for consistent memory management
- Leaf nodes are initialized at level 0 (the bottom level of the B-tree)
- The next pointer is initialized to NULL and will be used to maintain a linked list of leaf nodes
- Memory usage tracking is automatically updated when the node is allocated
- The node starts with zero items, ready for population with compressed integer data