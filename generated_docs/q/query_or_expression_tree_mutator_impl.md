# query_or_expression_tree_mutator_impl

## Location
[src/backend/nodes/nodeFuncs.c:3933-3963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L3933-L3963)

## Overview
A hybrid tree mutator function that intelligently chooses between query tree mutation and direct expression mutation based on the node type.

## Definition

```c
Node *
query_or_expression_tree_mutator_impl(Node *node,
									  tree_mutator_callback mutator,
									  void *context,
									  int flags)
```
## Detailed Description
The  function provides a unified entry point for tree mutation operations that can handle both Query nodes and expression nodes. It acts as a dispatcher that determines the appropriate mutation strategy based on the input node type. If the node is a Query, it delegates to the specialized  function; otherwise, it directly applies the mutator callback.

This hybrid approach mirrors the walker counterpart and is particularly useful when the caller needs to start mutation but doesn't know in advance whether they're dealing with a complete query or just an expression tree. It eliminates the need for callers to check node types and choose the appropriate mutator function, providing a clean and consistent interface for tree transformation operations.

## Parameters / Member Variables
- `*node`: The root node to mutate (can be either a Query node or any other expression node)
- `mutator`: Callback function that defines the mutation behavior for each visited node
- `*context`: Opaque context pointer passed through to the mutator callback
- `flags`: Control flags that modify mutation behavior
## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - query_tree_mutator (specialized mutator for Query nodes)
- Called from (representative examples):
  - query_or_expression_tree_mutator (wrapper function)
  - planstate_tree_walker (indirectly via wrapper)

## Notes and Other Information
- Returns a potentially modified Node pointer (may be the original node if unchanged)
- The hybrid design allows for flexible tree transformation without requiring caller to determine node type
- Particularly useful when the mutator's normal state change behavior is not appropriate for the outermost Query node
- Part of PostgreSQL's comprehensive node transformation infrastructure
- Complements the walker version by providing mutation capabilities with the same hybrid interface design
- Located in src/backend/nodes/nodeFuncs.c:3933-3963