# pull_exec_paramids_walker

## Location
src/backend/partitioning/partprune.c: 3356 - 3379

## Overview
A tree walker function that recursively traverses expression nodes to identify and collect PARAM_EXEC parameter IDs into a Bitmapset.

## Definition


## Detailed Description
This function implements the core logic for traversing PostgreSQL expression trees to find execution parameters. It follows the standard walker pattern used throughout PostgreSQL's expression processing system. When it encounters a Param node with paramkind = PARAM_EXEC, it adds the parameter ID to the context Bitmapset. The function recursively processes child nodes using expression_tree_walker to ensure complete coverage of the expression tree.

The walker returns false to indicate that traversal should continue, which is the typical pattern for collection operations where all nodes need to be visited.

## Parameters / Member Variables
- : Current node in the expression tree being examined
- : Pointer to Bitmapset that accumulates found PARAM_EXEC parameter IDs

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - [bms_add_member](../b/bms_add_member.md)
  - expression_tree_walker
  - [pull_exec_paramids_walker](pull_exec_paramids_walker.md) (recursive call)
  - Constants: PARAM_EXEC
  - Types: Param, Node
- Called from:
  - [pull_exec_paramids](pull_exec_paramids.md)
  - [pull_exec_paramids_walker](pull_exec_paramids_walker.md) (recursive)

## Notes and Other Information
- This is a static utility function implementing the walker pattern common in PostgreSQL
- The function uses the standard expression_tree_walker infrastructure for safe tree traversal
- PARAM_EXEC parameters represent values that are computed during query execution and may affect partition pruning
- The recursive nature ensures all nested expressions are properly analyzed
- Returns false to continue tree traversal, which is the standard pattern for collection walkers