# pull_varnos_walker

## Location
src/backend/optimizer/util/var.c: 155 - 290

## Overview
The core walker function that traverses expression trees to collect variable range table numbers (varnos), handling different node types including Vars, PlaceHolderVars, and CurrentOfExpr nodes.

## Definition
```c
static bool pull_varnos_walker(Node *node, pull_varnos_context *context)
```

## Detailed Description
The `pull_varnos_walker` function is the workhorse of the varno extraction system in PostgreSQL's query planner. It implements a tree walker pattern that recursively traverses expression trees and collects variable range table numbers based on the target sublevel specified in the context.

The function handles several node types specially:

1. **Var nodes**: Extracts the varno and any nulling relations if the variable is at the target sublevel
2. **CurrentOfExpr nodes**: Adds the cursor variable number for level-zero queries
3. **PlaceHolderVar nodes**: Complex handling that considers evaluation contexts, with special logic for translated appendrel PHVs
4. **Query nodes**: Recursively processes subqueries with appropriate level adjustment

The PlaceHolderVar handling is particularly sophisticated, dealing with ph_eval_at computation, appendrel translation, and fallback scenarios when PlaceHolderInfo is not yet available.

## Parameters / Member Variables
- `node`: The current Node being examined in the tree traversal
- `context`: Walker context containing:
  - `varnos`: Accumulating bitmap of discovered varnos
  - `root`: PlannerInfo for accessing PlaceHolderInfo
  - `sublevels_up`: Target query nesting level

## Dependencies
- Functions called/Symbols referenced:
  - bms_add_member, bms_add_members (bitmapset operations)
  - bms_equal, bms_difference, bms_join (bitmapset comparisons/operations)
  - query_tree_walker, expression_tree_walker (tree traversal)
  - pull_varnos_context (walker context structure)
  - CurrentOfExpr, PlaceHolderVar, PlaceHolderInfo (node types)
- Called from (representative examples):
  - pull_varnos
  - pull_varnos_of_level
  - pull_varnos_walker (recursive calls)

## Notes and Other Information
- Implements the visitor pattern for expression tree traversal
- Handles complex PlaceHolderVar scenarios including appendrel translation
- Uses bitmapset operations for efficient varno collection
- Properly manages query nesting levels through sublevels_up tracking
- Returns false to continue traversal, except when PlaceHolderVar processing is complete
- Critical component of PostgreSQL's query planning infrastructure for variable analysis