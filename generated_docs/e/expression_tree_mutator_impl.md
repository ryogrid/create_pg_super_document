# expression_tree_mutator_impl

## Location
[src/backend/nodes/nodeFuncs.c:2933-2941](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L2933-L2941)

## Overview
This function creates modified copies of expression trees, allowing nodes to be added, removed, or replaced while preserving the original tree structure through recursive copying.

## Definition
```c
Node *expression_tree_mutator_impl(Node *node,
                                  tree_mutator_callback mutator,
                                  void *context)
```

## Detailed Description
The `expression_tree_mutator_impl` function is the core implementation for creating modified copies of PostgreSQL expression trees. It handles the mechanical aspects of node copying while delegating specific transformation logic to user-provided mutator callbacks. The function supports a comprehensive set of PostgreSQL node types including expressions, operators, functions, aggregates, window functions, and various other SQL constructs.

The function works by examining each node's type and creating a shallow copy (FLATCOPY) of the node structure, then recursively calling the mutator function on each subnode that contains expressions. This ensures that transformations can be applied at any level of the expression tree while maintaining proper memory management and node relationships.

The function includes extensive documentation explaining the proper usage pattern: mutator functions should handle special cases for specific node types and then call `expression_tree_mutator` for standard recursive processing. This design allows mutators to have full control over the transformation process while avoiding the complexity of handling all possible node types.

## Parameters / Member Variables
- `node`: Pointer to the Node to be mutated/copied
- `mutator`: Callback function of type tree_mutator_callback that performs specific transformations
- `context`: User-defined context data passed to mutator callbacks

## Dependencies
- Functions called/Symbols referenced:
  - FLATCOPY (macro for shallow node copying)
  - MUTATE (macro for calling mutator on subnodes)
  - copyObject (for simple node types)
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - nodeTag (node type identification)
  - Various PostgreSQL node types (Var, Const, Param, Aggref, FuncExpr, etc.)
- Called from (representative examples):
  - expression_tree_mutator (inline wrapper)
  - planstate_tree_walker
  - Various query transformation functions

## Notes and Other Information
- The function creates copies rather than modifying nodes in-place, ensuring the original tree remains unchanged
- Includes stack depth checking to prevent overflow on deeply nested expressions
- Handles special cases for frequently used nodes (Var, Const) with optimized copying
- For SubLink nodes, it recurses into testexpr but preserves the link to sub-Query nodes unless the mutator handles Query nodes specially
- [SubPlan](../S/SubPlan.md) nodes have their testexpr and args processed, but the inner plan link is simply copied
- The function covers all node types typically found in target lists and qualifier clauses during query planning
- Uses efficient memory allocation with palloc and memcpy for node copying
- Returns NULL for NULL input nodes
- Terminates with error for unrecognized node types to ensure comprehensive coverage