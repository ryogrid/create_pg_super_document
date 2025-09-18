# pull_var_clause_walker

## Location
src/backend/optimizer/util/var.c: 627 - 743

## Overview
A static tree walker function that implements the core logic for extracting Var nodes and other specified node types from an expression tree, with configurable behavior for aggregates, window functions, and placeholders.

## Definition
```c
static bool pull_var_clause_walker(Node *node, pull_var_clause_context *context)
```

## Detailed Description
This function performs the actual tree traversal for `pull_var_clause()`, examining each node and taking appropriate actions based on the node type and the flags specified in the context. It implements sophisticated handling for different classes of nodes that may contain or represent variables.

**Node Type Handling:**

- **Var nodes**: Added directly to the output list. Validates that `varlevelsup == 0` (no upper-level variables expected).

- **Aggref nodes**: Behavior depends on flags:
  - `PVC_INCLUDE_AGGREGATES`: Add the Aggref to the list and stop traversing into it
  - `PVC_RECURSE_AGGREGATES`: Continue traversing into the aggregate's arguments 
  - Neither: Error if found
  - Validates that `agglevelsup == 0`

- **GroupingFunc nodes**: Treated identically to Aggref nodes, using the same PVC_*_AGGREGATES flags and level validation.

- **WindowFunc nodes**: Behavior depends on flags:
  - `PVC_INCLUDE_WINDOWFUNCS`: Add the WindowFunc to the list and stop traversing
  - `PVC_RECURSE_WINDOWFUNCS`: Continue traversing into the function's arguments
  - Neither: Error if found
  - Note: WindowFuncs have no levelsup field to validate

- **PlaceHolderVar nodes**: Behavior depends on flags:
  - `PVC_INCLUDE_PLACEHOLDERS`: Add the PlaceHolderVar to the list and stop traversing
  - `PVC_RECURSE_PLACEHOLDERS`: Continue traversing into the placeholder's expression
  - Neither: Error if found
  - Validates that `phlevelsup == 0`

The function maintains the invariant that upper-level references (levelsup > 0) should not be present, throwing errors when violations are detected.

## Parameters / Member Variables
- `node`: The current node being examined in the expression tree traversal
- `context`: Pointer to the context structure containing:
  - `varlist`: The output list where matching nodes are accumulated
  - `flags`: Bitmask controlling behavior for different node types

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - elog (for error reporting)
  - lappend (for adding nodes to the output list)
  - expression_tree_walker (for recursing into child nodes)
- Constants used:
  - PVC_INCLUDE_AGGREGATES, PVC_RECURSE_AGGREGATES
  - PVC_INCLUDE_WINDOWFUNCS, PVC_RECURSE_WINDOWFUNCS
  - PVC_INCLUDE_PLACEHOLDERS, PVC_RECURSE_PLACEHOLDERS
- Node types handled:
  - Var, Aggref, GroupingFunc, WindowFunc, PlaceHolderVar
- Data structures used:
  - pull_var_clause_context
- Called from:
  - pull_var_clause (main entry point)
  - pull_var_clause_walker (recursive self-calls via expression_tree_walker)

## Notes and Other Information
- This is a static helper function, not part of the public API
- The function never returns true (always continues or delegates to expression_tree_walker)
- Enforces strict validation that no upper-level references should be present
- Uses lappend to build the result list, maintaining order of discovery
- GroupingFunc nodes are treated as a special case of aggregate functions
- WindowFunc handling is unique in not having level validation since WindowFuncs lack a levelsup field
- When INCLUDE flags are used, the function stops recursing into that node type
- When RECURSE flags are used, traversal continues to find Vars within the node's arguments
- The "fall through" pattern allows RECURSE flags to continue to the general expression_tree_walker call