# find_placeholders_in_expr

## Location
src/backend/optimizer/util/placeholder.c: 257 - 299

## Overview
Discovers all PlaceHolderVars within a given expression tree and ensures that PlaceHolderInfo entries are created for each one found.

## Definition
```c
static void find_placeholders_in_expr(PlannerInfo *root, Node *expr)
```

## Detailed Description
The `find_placeholders_in_expr` function performs expression tree traversal to locate PlaceHolderVar nodes embedded within complex expressions. It uses PostgreSQLs `pull_var_clause` utility function with specific flags to extract all variable-like nodes, including PlaceHolderVars, from the expression tree. The function then filters the results to process only PlaceHolderVar nodes, calling `find_placeholder_info` for each to ensure proper registration with the optimizer.

The function is designed to handle complex expressions that may contain PlaceHolderVars nested within aggregates, window functions, and other expression types. It serves as a key component in the placeholder discovery process, ensuring that all PlaceHolderVars are properly catalogued before query optimization proceeds.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and placeholder management
- `expr`: Expression tree node to search for embedded PlaceHolderVars

## Dependencies
- Functions called/Symbols referenced:
  - pull_var_clause (for extracting variable-like nodes from expressions)
  - find_placeholder_info (for registering discovered PlaceHolderVars)
  - list_free (for memory cleanup)
  - PlaceHolderVar (node type checking and casting)
  - PVC_RECURSE_AGGREGATES, PVC_RECURSE_WINDOWFUNCS, PVC_INCLUDE_PLACEHOLDERS (flags for pull_var_clause)
- Called from (representative examples):
  - find_placeholder_info (for processing nested PlaceHolderVars)
  - find_placeholders_recurse (for processing join qualifications)

## Notes and Other Information
- Static function, only accessible within placeholder.c
- Uses pull_var_clause with comprehensive flags to ensure deep expression traversal
- Filters results to process only PlaceHolderVar nodes, ignoring regular Vars
- Handles expressions containing aggregates and window functions through appropriate flags
- Essential for discovering PlaceHolderVars that may be deeply nested in complex expressions
- Properly manages memory by freeing the temporary variable list
- May trigger recursive calls through find_placeholder_info when nested PlaceHolderVars are found