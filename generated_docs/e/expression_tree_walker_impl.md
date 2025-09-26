# expression_tree_walker_impl

## Location
src/backend/nodes/nodeFuncs.c: 2083 - 2096

## Overview
The core implementation function that provides generic tree-walking logic for traversing PostgreSQL expression trees in a read-only fashion.

## Definition

```c
bool
expression_tree_walker_impl(Node *node,
							tree_walker_callback walker,
							void *context)
```
## Detailed Description
This function is the heart of PostgreSQL's expression tree traversal infrastructure. It implements a comprehensive switch statement that handles dozens of different node types, recursively calling the provided walker function on all expression subnodes. The function eliminates the need for duplicate tree-walking code across different routines by providing a centralized, well-tested implementation.

The function uses two key macros:
- : Calls the walker function on a single node
- : Recursively processes list nodes by calling expression_tree_walker_impl

Key behaviors:
- Returns false to continue traversal, true to abort and propagate upward
- Guards against stack overflow with check_stack_depth()
- Handles primitive nodes (Var, Const, etc.) with no recursion
- Processes complex nodes by walking their expression subnodes
- For SubLink nodes, walks testexpr and calls walker on the sub-Query
- For List nodes, recurses directly without calling the walker

The function handles all major expression node types including operators, functions, aggregates, window functions, subqueries, joins, and many specialized constructs.

## Parameters / Member Variables
- : The current node being processed in the expression tree
- : Callback function that processes nodes and returns bool for continuation control
- : Arbitrary context data passed through to walker calls

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - nodeTag
  - elog
  - Various macros: WALK, LIST_WALK, IsA
- Called from (representative examples):
  - expression_tree_walker (macro wrapper)
  - planstate_tree_walker
  - LIST_WALK (recursive calls)

## Notes and Other Information
- This is the implementation function; most code uses the expression_tree_walker macro wrapper
- Supports read-only traversal and in-place node modification but not node replacement
- Comprehensive coverage of PostgreSQL expression node types
- Essential infrastructure used throughout the query planner and optimizer
- Companion to expression_tree_mutator for tree modification scenarios
- The walker callback controls traversal: false continues, true aborts
- Includes stack depth protection against deeply nested expressions