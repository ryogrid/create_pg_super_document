# rangeTableEntry_used

## Location
src/backend/rewrite/rewriteManip.c: 967 - 998

## Overview
A high-level interface function that determines whether a specific range table entry is referenced anywhere within a query tree or expression tree.

## Definition


## Detailed Description
This function serves as the main entry point for detecting whether a specific range table entry is referenced within a query or expression. It sets up the context structure and initiates a tree walk using query_or_expression_tree_walker, which can handle both Query nodes and bare expression trees without incorrectly incrementing the sublevels_up counter.

The function is commonly used during query rewriting and rule processing to determine if range table entries can be safely removed or if they must be preserved due to active references. This is particularly important for optimizing queries by eliminating unused table references and for validating rule transformations.

The function uses the rangeTableEntry_used_walker to perform the actual traversal and reference detection, properly handling various types of references including direct variable references, cursor references, range table references, and join references.

## Parameters / Member Variables
- : The root node (Query or expression) to search within
- : The range table index to search for references to
- : The initial sublevel offset (usually 0 for the current query level)

## Dependencies
- Functions called/Symbols referenced:
  - rangeTableEntry_used_context (context structure for walker)
  - query_or_expression_tree_walker (tree traversal function)
  - [rangeTableEntry_used_walker](rangeTableEntry_used_walker.md) (worker function that does the actual checking)
- Called from (representative examples):
  - [transformRuleStmt](../t/transformRuleStmt.md) (during rule statement transformation)
  - [rewriteRuleAction](rewriteRuleAction.md) (during rule action rewriting)
  - [matchLocks](../m/matchLocks.md) (during rule lock matching)
  - [fireRIRrules](../f/fireRIRrules.md) (during rules in rules processing)
  - [ReplaceVarsNoMatchOption](../R/ReplaceVarsNoMatchOption.md) (during variable replacement operations)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:967-998
- Returns true if the range table entry is referenced, false otherwise
- Part of PostgreSQL's query rewriting infrastructure
- Essential for rule processing and query optimization
- Uses query_or_expression_tree_walker instead of query_tree_walker to properly handle both Query nodes and expression trees
- The function correctly manages sublevel counting to avoid false positives when checking references across query nesting boundaries