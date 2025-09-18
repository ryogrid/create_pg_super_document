# transformPLAssignStmt

## Location
[src/backend/parser/analyze.c:2619-2867](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L2619-L2867)

## Overview
Transforms a PL/pgSQL assignment statement into a SELECT query that computes the new value and handles type coercion and indirection operations.

## Definition


## Detailed Description
This function transforms a PL/pgSQL assignment statement into a Query structure representing a SELECT statement. The transformation handles both simple assignments and complex assignments involving field access or array subscripting through indirection. The function performs type checking and coercion using PL/pgSQL-specific coercion rules (COERCION_PLPGSQL) rather than standard SQL assignment coercion.

The transformation process involves:
1. Building a ColumnRef for the target variable, handling multi-part names
2. Transforming the target reference to get type information
3. Processing the SELECT statement that provides the assignment value
4. Performing type coercion between the source and target types
5. Handling indirection operations for field stores and array assignments
6. Processing standard SELECT clauses (WHERE, GROUP BY, ORDER BY, etc.)

## Parameters / Member Variables
- : Parse state containing context information for the transformation
- : The PL/pgSQL assignment statement to transform, containing:
  - : Target variable name
  - : Number of dotted names in the target
  - : List of field/array access operations
  - : SelectStmt providing the assignment value
  - : Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - makeNode, makeString, list_make1, list_copy, list_delete_first
  - [transformExpr](transformExpr.md), transformFromClause, transformTargetList
  - [transformAssignmentIndirection](transformAssignmentIndirection.md), coerce_to_target_type
  - [transformWhereClause](transformWhereClause.md), transformSortClause, transformGroupClause
  - [transformDistinctClause](transformDistinctClause.md), transformDistinctOnClause
  - [transformLimitClause](transformLimitClause.md), transformWindowDefinitions
  - [transformLockingClause](transformLockingClause.md), assign_query_collations, parseCheckAggregates
  - exprType, exprTypmod, exprCollation, exprLocation
  - [format_type_be](../f/format_type_be.md), makeFromExpr
- Called from (representative examples):
  - [transformStmt](transformStmt.md)

## Notes and Other Information
- Uses COERCION_PLPGSQL instead of COERCION_ASSIGNMENT for type coercion
- Handles composite types specially to maintain backwards compatibility
- The function expects exactly one item in the SELECT target list
- Supports complex assignments through indirection (field access, array subscripting)
- Processes standard SELECT features like WHERE, GROUP BY, ORDER BY, LIMIT, DISTINCT, and window functions
- Performs aggregate function validation if aggregates are present in the query