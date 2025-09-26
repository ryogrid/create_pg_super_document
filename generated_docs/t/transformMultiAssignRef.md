# transformMultiAssignRef

## Location
[src/backend/parser/parse_expr.c:1484-1631](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L1484-L1631)

## Overview
Transforms a multi-assignment reference node (MultiAssignRef) used in UPDATE statements with multiple column assignments from subqueries or row expressions.

## Definition
```c
static Node *transformMultiAssignRef(ParseState *pstate, MultiAssignRef *maref)
```

## Detailed Description
The transformMultiAssignRef function handles transformation of multi-column assignments in UPDATE statements, supporting syntax like `UPDATE table SET (col1, col2, col3) = subquery` or `UPDATE table SET (col1, col2, col3) = ROW(val1, val2, val3)`. It processes both SubLink (subqueries) and RowExpr (row expressions) sources.

The function operates in two phases:
1. **First column processing (colno == 1)**: Transforms the source expression and validates column count
   - For SubLinks: Relabels as MULTIEXPR_SUBLINK, validates column count, and stores in p_multiassign_exprs
   - For RowExprs: Transforms with SetToDefault support, validates column count, and stores temporarily
2. **Subsequent columns (colno > 1)**: Extracts the appropriate column value from the previously stored expression
   - For SubLinks: Creates a PARAM_MULTIEXPR parameter referencing the subquery column
   - For RowExprs: Extracts the corresponding element from the row expression

## Parameters / Member Variables
- `pstate`: ParseState context containing parsing state and multi-assignment expression storage
- `maref`: MultiAssignRef node containing source expression, column number, total columns, and position information

## Dependencies
- Functions called/Symbols referenced:
  - MultiAssignRef, SubLink, RowExpr (struct types for multi-assignment references)
  - EXPR_KIND_UPDATE_SOURCE (expression context for UPDATE sources)
  - EXPR_SUBLINK, MULTIEXPR_SUBLINK (sublink type constants)
  - PARAM_MULTIEXPR (parameter type for multi-expression references)
  - transformExprRecurse (recursively transforms expressions)
  - transformRowExpr (transforms row expressions with special handling)
  - count_nonjunk_tlist_entries (counts non-junk target list entries)
  - makeTargetEntry, makeNode (node creation functions)
  - exprType, exprTypmod, exprCollation, exprLocation (expression metadata functions)
- Called from:
  - transformExprRecurse (main expression transformation dispatcher)

## Notes and Other Information
- This function is specific to UPDATE statement processing and only operates in EXPR_KIND_UPDATE_SOURCE context
- Supports two source types: SubLinks (subqueries) and RowExprs (row constructors)
- Uses p_multiassign_exprs list to track transformed expressions across multiple column references
- Creates PARAM_MULTIEXPR parameters to reference subquery columns efficiently
- Validates that the number of source columns matches the number of target columns
- For RowExprs, cleans up the temporary storage when processing the last column
- The function is static, indicating it's only used within the parse_expr.c module
- Error handling includes syntax errors for column count mismatches and unsupported source types