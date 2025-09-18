# transformSetOperationTree

## Location
src/backend/parser/analyze.c: 2003 - 2333

## Overview
Recursively transforms leaves and internal nodes of a set-operation tree, converting SelectStmt nodes into either subqueries (RangeTblRef) or SetOperationStmt nodes.

## Definition


## Detailed Description
transformSetOperationTree is the core recursive function that processes set operation trees (UNION, INTERSECT, EXCEPT). It handles both leaf nodes (individual SELECT statements) and internal nodes (set operation nodes) differently based on their characteristics.

For leaf nodes (including internal nodes that have ORDER BY, LIMIT, or other clauses that require independent processing), the function transforms the SelectStmt into a complete Query using parse_sub_analyze, then creates a range table entry for this subquery and returns a RangeTblRef pointing to it.

For internal set operation nodes, the function creates a SetOperationStmt node and recursively processes both left and right child nodes. It performs critical type reconciliation by determining common types, type modifiers, and collations for each corresponding column position across the left and right operands. The function also handles special cases like UNKNOWN-type constants and parameters by coercing them to the resolved common types when safe to do so.

The function validates that both operands have the same number of columns and establishes grouping clauses for duplicate elimination (except for UNION ALL). For recursive CTEs, it requires hash-capable operators to support the recursive processing algorithm.

## Parameters / Member Variables
- : ParseState structure containing parsing context and range table information
- : SelectStmt node representing the set operation tree node to transform
- : Boolean indicating if this is the top-level call (used for recursive CTE processing)
- : Output parameter returning target list entries for type processing (NULL for external callers)

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - parse_sub_analyze (subquery transformation)
  - contain_vars_of_level/locate_var_of_level (variable reference validation)
  - addRangeTableEntryForSubquery (subquery RTE creation)
  - makeAlias (alias creation for subqueries)
  - determineRecursiveColTypes (recursive CTE type determination)
  - select_common_type/select_common_typmod/select_common_collation (type reconciliation)
  - coerce_to_common_type (type coercion validation and UNKNOWN handling)
  - makeSortGroupClauseForSetOp (grouping clause creation)
  - makeTargetEntry (dummy target entry creation)
- Called from (representative examples):
  - transformSetOperationStmt (top-level set operation processing)
  - transformSetOperationTree (recursive self-calls for left and right operands)

## Notes and Other Information
- The function includes stack depth checking to prevent overflow from deeply nested set operations
- INTO clauses are only allowed in the leftmost SELECT of a set operation tree
- FOR UPDATE/SHARE clauses are not supported with set operations
- Internal nodes with ORDER BY, LIMIT, or WITH clauses are treated as leaf nodes requiring independent subquery processing
- Type coercion is handled carefully: non-UNKNOWN types are validated but not modified to preserve child query semantics, while UNKNOWN constants/parameters are coerced to resolved types when safe
- Common collation is required for all set operators except UNION ALL per SQL standard
- Recursive CTEs require hash-capable grouping operators for proper duplicate elimination
- The function creates dummy target list entries using SetToDefault nodes to carry type information up the recursion tree
- Error positioning callbacks ensure accurate error reporting during type resolution