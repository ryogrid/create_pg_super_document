# pull_vars_walker

## Location
[src/backend/optimizer/util/var.c:355-402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L355-L402)

## Overview
A tree walker callback function that identifies and collects Var and PlaceHolderVar nodes at a specific query nesting level.

## Definition


## Detailed Description
The  function serves as a callback for tree walking operations, specifically designed to identify and collect variable references (Var nodes) and placeholder variables (PlaceHolderVar nodes) that belong to a target query nesting level. This function implements the core logic for the variable extraction process used by .

The function handles different node types:
- **Var nodes**: Checks if the variable's  matches the target level and adds it to the collection
- **PlaceHolderVar nodes**: Similar to Var nodes, but checks  and doesn't recurse into the contained expression
- **Query nodes**: Adjusts the nesting level context and recursively processes subqueries
- **Other nodes**: Uses  for standard recursive processing

The function maintains proper nesting level tracking when encountering Query nodes by incrementing  before recursion and decrementing it afterward.

## Parameters / Member Variables
- : The current node being processed in the tree traversal
- : Pointer to pull_vars_context structure containing:
  - : List to accumulate found variables
  - : Target query nesting level being searched for

## Dependencies
- Functions called/Symbols referenced:
  - IsA (node type checking macros)
  - lappend (list append function)
  - query_tree_walker (recursive query tree traversal)
  - expression_tree_walker (recursive expression tree traversal)
  - Var, PlaceHolderVar (node type structures)
- Called from (representative examples):
  - [pull_vars_of_level](pull_vars_of_level.md) (primary caller via query_or_expression_tree_walker)
  - [pull_vars_walker](pull_vars_walker.md) (recursive self-calls)

## Notes and Other Information
- This is a static function, internal to var.c
- Implements the visitor pattern for tree traversal
- Handles proper scope management for nested queries
- Does not copy variables, only links them into the result list
- [PlaceHolderVar](../P/PlaceHolderVar.md) nodes are treated specially - their contained expressions are not traversed
- Returns false to continue tree traversal, true would halt traversal early