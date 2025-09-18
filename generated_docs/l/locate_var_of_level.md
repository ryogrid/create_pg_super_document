# locate_var_of_level

## Location
[src/backend/optimizer/util/var.c:509-524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L509-L524)

## Overview
Finds the parse location (source position) of any Var node at a specified query nesting level within an expression tree, primarily used for error reporting.

## Definition
```c
int locate_var_of_level(Node *node, int levelsup)
```

## Detailed Description
This function traverses an expression tree to locate any Var node at the specified query nesting level and returns its parse location for error reporting purposes. The function uses a context structure to track the search state and employs the standard PostgreSQL tree walker pattern.

The function returns the parse location (character position in the original SQL text) of the first Var found at the target level, or -1 if:
- No such Var exists in the tree (likely caller error)
- All Vars at that level have unknown parse locations

Unlike `contain_vars_of_level()` which only checks for existence, this function extracts location information for diagnostic purposes. The function can recurse into sublinks and can be invoked directly on Query nodes.

## Parameters / Member Variables
- `node`: The root node of the expression tree to search
- `levelsup`: The target query nesting level to search for (0 = current level, 1 = one level up, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - query_or_expression_tree_walker (for traversing the expression tree)
  - [locate_var_of_level_walker](locate_var_of_level_walker.md) (the actual tree walker implementation)
- Data structures used:
  - [locate_var_of_level_context](locate_var_of_level_context.md) (context structure for tracking search state)
- Called from:
  - [transformSetOperationTree](../t/transformSetOperationTree.md) (src/backend/parser/analyze.c:2093)
  - [check_agg_arguments](../c/check_agg_arguments.md) (src/backend/parser/parse_agg.c:703)
  - [checkExprIsVarFree](../c/checkExprIsVarFree.md) (src/backend/parser/parse_clause.c:1935)

## Notes and Other Information
- Designed specifically for error reporting scenarios where location information is needed
- Returns the first matching Var found during traversal - the order depends on the tree walker traversal
- The function comment notes that it might seem logical to merge this with `contain_vars_of_level()`, but this would complicate that function's simpler API
- Performance is not critical since this is primarily used for error reporting
- The context structure contains: `var_location` (result field, initialized to -1) and `sublevels_up` (target level)