# ChangeVarNodes_walker

## Location
[src/backend/rewrite/rewriteManip.c:565-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L565-L674)

## Overview
A static walker function that recursively traverses expression trees to update range table references, changing all references from an old range table index to a new one at a specific sublevel.

## Definition
```c
static bool ChangeVarNodes_walker(Node *node, ChangeVarNodes_context *context)
```

## Detailed Description
This function is the core worker function for the ChangeVarNodes operation. It implements a tree-walking algorithm that visits each node in an expression tree and updates various types of range table references from an old index to a new index. The function handles multiple PostgreSQL node types including Var nodes, CurrentOfExpr, RangeTblRef, JoinExpr, PlaceHolderVar, PlanRowMark, and AppendRelInfo nodes.

The function operates at a specific sublevel context, allowing it to correctly handle nested subqueries and correlated references. It uses the expression_tree_walker and query_tree_walker infrastructure to ensure complete traversal of the tree structure.

## Parameters / Member Variables
- `node`: The current node being processed in the tree traversal
- `context`: A ChangeVarNodes_context structure containing:
  - `rt_index`: The original range table index to be replaced
  - `new_index`: The new range table index to replace the old one
  - `sublevels_up`: The sublevel at which to perform the replacement

## Dependencies
- Functions called/Symbols referenced:
  - [adjust_relid_set](../a/adjust_relid_set.md)
  - query_tree_walker
  - expression_tree_walker
  - IsA (macro for node type checking)
- Called from (representative examples):
  - [ChangeVarNodes](ChangeVarNodes.md) (recursive self-call through walker infrastructure)
  - query_tree_walker (for subquery traversal)
  - expression_tree_walker (for expression traversal)

## Notes and Other Information
- This is a static function used only within rewriteManip.c
- The function handles sublevel tracking to ensure correct operation in nested subqueries
- It includes assertions to verify that certain planner auxiliary nodes (SpecialJoinInfo, PlaceHolderInfo, MinMaxAggInfo) are not encountered at this stage
- For Query nodes, it increments the sublevel counter before recursing and decrements it afterward
- The function updates not just the primary range table references but also related nulling relations in Var and PlaceHolderVar nodes
- Returns false to continue tree traversal in most cases