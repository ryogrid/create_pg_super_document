# OffsetVarNodes_walker

## Location
src/backend/rewrite/rewriteManip.c: 392 - 480

## Overview
A static tree walker function that adjusts variable node numbers and relation identifiers by a specified offset, handling various node types in expression trees and query structures.

## Definition


## Detailed Description
This function implements a recursive tree walker that adjusts relation identifiers throughout expression trees and query structures. It handles multiple node types including Var nodes, CurrentOfExpr, RangeTblRef, JoinExpr, PlaceHolderVar, and AppendRelInfo. The function respects query nesting levels (sublevels_up) to ensure that only variables at the appropriate query level are modified. For Var nodes, it adjusts varno, varnullingrels, and varnosyn. The function also handles subqueries by recursively calling itself with adjusted context levels.

## Parameters / Member Variables
- `node`: The current Node being processed in the tree traversal
- `context`: OffsetVarNodes_context structure containing offset value and current sublevel information

## Dependencies
- Functions called/Symbols referenced:
  - IsA (type checking macro)
  - offset_relid_set (for adjusting relation ID sets)
  - query_tree_walker (for Query node recursion)
  - expression_tree_walker (for general expression recursion)
  - Assert (debugging assertions)
- Called from (representative examples):
  - OffsetVarNodes (main entry point)
  - OffsetVarNodes_walker (recursive self-calls)

## Notes and Other Information
- This is a static function used internally by the OffsetVarNodes system
- Handles query nesting by tracking sublevels_up in the context
- Includes assertions to ensure it doesn't encounter unexpected planner auxiliary nodes
- Processes different node types with specific logic for each:
  - Var: adjusts varno, varnullingrels, and varnosyn
  - CurrentOfExpr: adjusts cvarno at top level only
  - RangeTblRef: adjusts rtindex at top level only  
  - JoinExpr: adjusts rtindex if present
  - PlaceHolderVar: adjusts phrels and phnullingrels
  - AppendRelInfo: adjusts parent_relid and child_relid
- Used during query rewriting when combining range tables or adjusting variable references
- Critical for maintaining correct variable-to-relation mappings during query transformation