# recurse_pushdown_safe

## Location
src/backend/optimizer/path/allpaths.c: 3638 - 3706

## Overview
Recursively traverses set operation trees (UNION, INTERSECT, EXCEPT) to check if qual pushdown is safe for all component subqueries in the tree structure.

## Definition
static bool recurse_pushdown_safe(Node *setOp, Query *topquery, pushdown_safety_info *safetyInfo)

## Detailed Description
This helper function implements recursive traversal of PostgreSQL's set operation trees to determine qual pushdown safety. It handles the complex nested structure of set operations where subqueries can be organized in tree form with UNION, INTERSECT, and EXCEPT operations.

The function processes two main node types:

1. **RangeTblRef nodes**: Represents leaf subqueries in the set operation tree. For these nodes, it extracts the actual subquery from the range table and delegates safety checking to subquery_is_pushdown_safe().

2. **SetOperationStmt nodes**: Represents internal nodes with set operations. The function applies specific rules:
   - **EXCEPT operations**: Immediately returns false as EXCEPT operations are incompatible with qual pushdown (as noted in subquery_is_pushdown_safe point 2)
   - **Other operations (UNION/INTERSECT)**: Recursively checks both left and right arguments, requiring both to be safe for pushdown

The recursive nature ensures that all components of complex nested set operations are evaluated for pushdown safety, maintaining the semantic correctness of the entire query tree.

## Parameters / Member Variables
- : Node representing either a RangeTblRef (leaf subquery) or SetOperationStmt (set operation)
- : Top-level query containing the complete set operation tree structure  
- : Structure to accumulate safety information and unsafe column flags across all components

## Dependencies
- Functions called/Symbols referenced:
  - subquery_is_pushdown_safe: Performs actual safety analysis for individual subqueries
  - rt_fetch: Retrieves range table entries from the query's range table
  - nodeTag: Determines the runtime type of nodes for error handling
  - recurse_pushdown_safe: Recursive self-calls for left and right set operation arguments
- Called from (representative examples):
  - subquery_is_pushdown_safe: When processing set operation trees
  - recurse_pushdown_safe: Recursive self-calls for tree traversal

## Notes and Other Information
- Essential component of PostgreSQL's qual pushdown optimization for set operations
- Implements tree traversal pattern common in query processing
- Enforces EXCEPT operation restriction at the structural level
- Accumulates safety information across all subqueries in the set operation tree
- Handles arbitrarily complex nested set operation structures
- Critical for maintaining correctness when optimizing queries with multiple UNION/INTERSECT/EXCEPT clauses
- Works in conjunction with subquery_is_pushdown_safe to provide comprehensive safety analysis