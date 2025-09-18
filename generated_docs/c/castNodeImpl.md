# castNodeImpl

## Location
src/include/nodes/nodes.h: 169 - 173

## Overview
The `castNodeImpl` function provides type-safe casting for PostgreSQL node structures with runtime type verification.

## Definition
```c
static inline Node *castNodeImpl(NodeTag type, void *ptr)
```

## Detailed Description
The `castNodeImpl` function performs a type-safe cast of a generic pointer to a Node pointer, with runtime verification that the node's actual type matches the expected type. This function serves as the implementation behind the `castNode` macro, providing a safety mechanism to prevent incorrect type casts in PostgreSQL's node system. The function allows NULL pointers to pass through safely, making it suitable for use in contexts where nodes may be optional.

The function uses an assertion to verify type correctness at runtime (in debug builds), helping to catch programming errors where nodes are incorrectly assumed to be of a particular type.

## Parameters / Member Variables
- `type`: The expected `NodeTag` that the pointer should have
- `ptr`: A generic pointer that should point to a node structure of the specified type (or NULL)

## Dependencies
- Functions called/Symbols referenced:
  - `nodeTag` (macro to extract the type tag from a node)
  - `Assert` (debugging assertion macro)
- Called from (representative examples):
  - `castNode` (macro wrapper)

## Notes and Other Information
- Handles NULL pointers gracefully by allowing them to pass through the type check
- The type verification is performed using an assertion, which means it only provides protection in debug builds
- The function is declared as `static inline` for performance optimization
- Should not be used directly; the `castNode(type, nodeptr)` macro should be used instead for better readability
- Part of PostgreSQL's type safety infrastructure for the node system
- Helps prevent runtime errors that could occur from incorrect node type assumptions
- Returns the input pointer cast to Node* type, allowing it to be further cast to the specific node type by the calling macro