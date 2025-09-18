# transformSetOperationStmt

## Location
src/backend/parser/analyze.c: 1699 - 1955

## Overview
Transforms a set-operations tree (UNION/INTERSECT/EXCEPT) into a Query containing the leaf SELECTs as subqueries and a top-level setOperations tree.

## Definition


## Detailed Description
transformSetOperationStmt handles the transformation of complex SELECT statements that involve set operations (UNION, INTERSECT, EXCEPT). The function builds a top-level Query structure that contains the individual SELECT statements as subqueries in its range table, with the set operation tree stored in the Query's setOperations field.

The transformation process involves several critical steps: First, it validates that no INTO clauses exist in inappropriate contexts and extracts top-level clauses (ORDER BY, LIMIT, locking) that must be handled at the outer level rather than recursively. It then calls transformSetOperationTree to recursively process the set operation tree structure.

After the tree transformation, the function constructs a dummy target list for the outer query using column names from the leftmost SELECT and common data types/collations determined by the set operations. This target list allows ORDER BY clauses to reference result columns by name or position. The function also creates a temporary namespace entry to enable proper column reference resolution in ORDER BY expressions.

A key restriction is that ORDER BY clauses can only reference result columns by name or number (SQL92-style), not arbitrary expressions, which is enforced by checking that no new target list entries are added during ORDER BY processing.

## Parameters / Member Variables
- : ParseState structure containing parsing context and range table information
- : SelectStmt node representing the set operation tree to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (Query creation)
  - transformWithClause (WITH clause processing)
  - transformSetOperationTree (recursive set operation tree processing)
  - rt_fetch (range table entry retrieval)
  - makeVar/makeTargetEntry (dummy target list construction)
  - addRangeTableEntryForJoin (temporary namespace creation for ORDER BY)
  - transformSortClause (ORDER BY processing)
  - transformLimitClause (LIMIT/OFFSET processing)
  - assign_query_collations (collation assignment)
  - parseCheckAggregates (aggregate validation)
- Called from (representative examples):
  - transformStmt (main statement transformation dispatcher)

## Notes and Other Information
- The function rejects FOR UPDATE/SHARE clauses with set operations as they are not currently supported
- INTO clauses are only allowed in the leftmost SELECT of a set operation tree
- The leftmost SELECT's column names are used for the result columns, while data types and collations come from the common types determined across all set operations
- A temporary Join RTE is created during ORDER BY processing to provide proper namespace resolution for result columns
- The function enforces SQL92 restrictions on ORDER BY clauses - only result column names/numbers are allowed, not expressions
- Memory management includes proper cleanup of temporary namespace entries and range table modifications
- The transformation preserves the original set operation tree structure in the Query's setOperations field for later processing by the planner