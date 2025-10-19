# newNode

## Location
[src/include/nodes/nodes.h:144-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/nodes.h#L144-L154)

## Overview
The `newNode` function is a low-level utility for creating new PostgreSQL node structures with proper memory allocation and type tagging.

## Definition
```c
static inline Node *newNode(size_t size, NodeTag tag)
```

## Detailed Description
The `newNode` function allocates zero-initialized memory for a new node structure and sets its type tag. It serves as the foundational building block for creating all node types in PostgreSQL's parse tree and execution plan structures. The function performs basic validation to ensure the allocated size is at least large enough to hold the base Node structure, which contains the essential type tag field.

**Important**: This function should not be used directly in most cases. Instead, the `makeNode` macro should be preferred as it automatically provides the correct size and type tag for specific node types.

## Parameters / Member Variables
- `size`: The number of bytes to allocate for the node structure, must be at least `sizeof(Node)`
- `tag`: The `NodeTag` value that identifies the specific type of node being created

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (memory allocation with zero initialization)
  - `Assert` (debugging assertion macro)
- Called from (representative examples):
  - `makeNode` (macro wrapper)
  - [_copyExtensibleNode](../c/_copyExtensibleNode.md)
  - [_readExtensibleNode](../r/_readExtensibleNode.md)

## Notes and Other Information
- Uses `palloc0` to ensure the allocated memory is zero-initialized
- Contains an assertion to validate that the size is sufficient for at least a base Node
- The function is declared as `static inline` for performance optimization
- Direct usage is discouraged; the `makeNode(type)` macro should be used instead for type safety and convenience
- Part of PostgreSQL's node system that provides a uniform interface for parse trees, plan trees, and other tree structures

## Simplified Source

```c
static inline Node *newNode(size_t size, NodeTag tag) {
    Node *result;

    // Ensure size is at least large enough for base Node structure
    Assert(size >= sizeof(Node));

    // Allocate zero-initialized memory and set type tag
    result = (Node *) palloc0(size);
    result->type = tag;

    return result;
}
```

This function creates a new PostgreSQL node by allocating zero-initialized memory of the specified size and setting the node's type tag. It's the foundation for all node creation in PostgreSQL's parse and execution trees, though the `makeNode` macro is typically used instead for convenience.