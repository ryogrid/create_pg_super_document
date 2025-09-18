# locate_var_of_level_walker

## Location
[src/backend/optimizer/util/var.c:525-606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L525-L606)

## Overview
A static tree walker function that traverses an expression tree to find the parse location of any Var node at a specified query nesting level, used internally by `locate_var_of_level()`.

## Definition
```c
static bool locate_var_of_level_walker(Node *node, locate_var_of_level_context *context)
```

## Detailed Description
This function implements the core traversal logic for `locate_var_of_level()`, searching through an expression tree to find the first Var node at the target query level that has a valid parse location. Unlike `contain_vars_of_level_walker()` which only checks for existence, this walker extracts and stores the location information.

The function handles different node types with location-specific logic:

- **Var nodes**: Checks if `varlevelsup` matches the target level and if the location is valid (>= 0). If both conditions are met, stores the location and returns true to abort traversal.
- **CurrentOfExpr nodes**: Returns false immediately since CurrentOfExpr doesn't carry location information.
- **PlaceHolderVar nodes**: No special handling needed - just traverses the contained expression.
- **Query nodes**: Recursively processes subqueries by adjusting the context's nesting level counter.

The function uses the standard PostgreSQL short-circuit tree walker pattern, returning true when a match is found to stop further traversal.

## Parameters / Member Variables
- `node`: The current node being examined in the expression tree traversal
- `context`: Pointer to the context structure containing:
  - `var_location`: Output field where the found location is stored
  - `sublevels_up`: Target query nesting level being searched for

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - query_tree_walker (for recursing into Query nodes)
  - expression_tree_walker (for recursing into expression nodes)
- Referenced node types:
  - Var
  - [CurrentOfExpr](../C/CurrentOfExpr.md)
  - [PlaceHolderVar](../P/PlaceHolderVar.md)
  - [Query](../Q/Query.md)
- Data structures used:
  - [locate_var_of_level_context](locate_var_of_level_context.md)
- Called from:
  - [locate_var_of_level](locate_var_of_level.md) (main entry point)
  - [locate_var_of_level_walker](locate_var_of_level_walker.md) (recursive self-calls)

## Notes and Other Information
- This is a static helper function, not part of the public API
- Only considers Vars with valid location information (location >= 0)
- Unlike the corresponding `contain_vars_of_level_walker()`, this function doesn't need special CurrentOfExpr handling since CurrentOfExpr nodes don't carry parse location data
- [PlaceHolderVar](../P/PlaceHolderVar.md) nodes are handled more simply than in the contain variant - no level checking, just traversal of contained expressions
- The context structure is modified in-place when managing subquery nesting levels
- Returns the location of the first matching Var encountered during traversal