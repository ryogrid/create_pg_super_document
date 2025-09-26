# intset_new_internal_node

## Location
[src/backend/lib/integerset.c:316-330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L316-L330)

## Overview
Allocates and initializes a new internal node for the IntegerSet B-tree structure.

## Definition
```c
static intset_internal_node * intset_new_internal_node(IntegerSet *intset)
```

## Detailed Description
The `intset_new_internal_node` function creates a new internal node for the IntegerSet's B-tree structure. Internal nodes serve as intermediate levels in the tree, containing keys and pointers to child nodes (either other internal nodes or leaf nodes). The function allocates memory in the IntegerSet's designated memory context and updates the memory usage tracking. The new node is initialized with default values, with the level to be set by the caller.

## Parameters / Member Variables
- `intset`: Pointer to the IntegerSet structure that will contain this internal node

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Memory allocation in specific context
  - `[GetMemoryChunkSpace](../G/GetMemoryChunkSpace.md)`: Function to get allocated memory size
  - [intset_internal_node](intset_internal_node.md): Structure type for internal nodes
- Called from (representative examples):
  - [intset_update_upper](intset_update_upper.md): Used when updating upper levels of the B-tree

## Notes and Other Information
- This is a static function, only accessible within the integerset.c file
- Memory is allocated in the IntegerSet's designated memory context to ensure consistent memory management
- The level field is initialized to 0 but must be set by the caller to the appropriate tree level
- Memory usage tracking is automatically updated when the node is allocated
- The node starts with zero items, ready for population by the caller