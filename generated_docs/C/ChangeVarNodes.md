# ChangeVarNodes

## Location
src/backend/rewrite/rewriteManip.c: 675 - 737

## Overview
A public function that changes all range table references from an old index to a new index within an expression tree or query tree.

## Definition
```c
void ChangeVarNodes(Node *node, int rt_index, int new_index, int sublevels_up)
```

## Detailed Description
ChangeVarNodes is the entry point for updating range table references throughout a query tree or expression tree. This function is essential during query rewriting operations where range table entries need to be renumbered or references need to be redirected to different relations.

The function sets up a context structure and delegates the actual tree walking to ChangeVarNodes_walker. It has special handling for Query nodes at the top level, where it must update additional Query-specific fields like resultRelation, mergeTargetRelation, exclRelIndex, and rowMarks entries when operating at sublevel 0.

The function can handle both bare expression trees and complete Query structures, automatically detecting the input type and choosing the appropriate traversal method.

## Parameters / Member Variables
- `node`: The root node of the tree to process (can be Query or expression node)
- `rt_index`: The original range table index to be replaced
- `new_index`: The new range table index to use as replacement
- `sublevels_up`: The sublevel at which to perform replacements (0 for current level)

## Dependencies
- Functions called/Symbols referenced:
  - ChangeVarNodes_walker
  - query_tree_walker
  - RowMarkClause
  - ChangeVarNodes_context (structure)
  - IsA (macro for node type checking)
- Called from (representative examples):
  - TriggerEnabled (trigger.c)
  - get_relation_info (plancat.c)
  - rewriteRuleAction (rewriteHandler.c)
  - ApplyRetrieveRule (rewriteHandler.c)
  - rewriteTargetView (rewriteHandler.c)
  - add_security_quals (rowsecurity.c)

## Notes and Other Information
- This is a public function declared in rewrite/rewriteManip.h
- Special handling for Query nodes includes updating resultRelation, mergeTargetRelation, onConflict->exclRelIndex, and rowMarks entries
- The function only updates Query-level fields when sublevels_up is 0, preventing incorrect updates during subquery recursion
- Widely used throughout the rewrite system for query transformation operations
- Essential for operations like view rewriting, rule application, and security policy enforcement
- The function is designed to be safe for both partial expression trees and complete query structures