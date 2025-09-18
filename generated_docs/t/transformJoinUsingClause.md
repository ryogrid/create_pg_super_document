# transformJoinUsingClause

## Location
src/backend/parser/parse_clause.c: 308 - 366

## Overview
Builds a complete ON clause from partially-transformed USING lists by creating equality conditions between corresponding left and right join columns.

## Definition


## Detailed Description
The `transformJoinUsingClause` function is a static helper function that converts JOIN USING clauses into equivalent ON clauses by creating equality comparisons between corresponding columns from the left and right sides of the join.

The function operates through several key steps:
1. **Variable pairing**: Uses `forboth` to iterate through corresponding left and right variable lists simultaneously
2. **Permission marking**: Marks each join variable as requiring SELECT privilege via `markVarForSelectPriv`
3. **Equality creation**: Constructs `lvar = rvar` equality expressions using `makeSimpleA_Expr`
4. **Condition combining**: Combines multiple equality conditions with AND if there are multiple join columns
5. **Expression transformation**: Applies `transformExpr` to fix up operators and ensure proper typing
6. **Boolean coercion**: Ensures the final result is properly coerced to boolean type

The function employs a "cheating" approach by building an untransformed operator tree with already-transformed Var leaves, which requires special handling by `transformExpr` and manual permission marking.

## Parameters / Member Variables
- `pstate`: The current parse state containing parsing context and permission tracking information
- `leftVars`: List of Var nodes representing columns from the left side of the join
- `rightVars`: List of Var nodes representing columns from the right side of the join (must correspond to leftVars)

## Dependencies
- Functions called/Symbols referenced:
  - forboth
  - [A_Expr](../A/A_Expr.md)
  - [markVarForSelectPriv](../m/markVarForSelectPriv.md)
  - makeSimpleA_Expr
  - AEXPR_OP
  - copyObject
  - [makeBoolExpr](../m/makeBoolExpr.md)
  - AND_EXPR
  - [transformExpr](transformExpr.md)
  - EXPR_KIND_JOIN_USING
  - [coerce_to_boolean](../c/coerce_to_boolean.md)
- Called from (representative examples):
  - [transformFromClauseItem](transformFromClauseItem.md)

## Notes and Other Information
- This is a static (internal) function within parse_clause.c, not exposed in the public API
- Uses a "cheating" approach with untransformed operators and pre-transformed leaves
- Automatically handles permission marking for SELECT privileges on join columns
- Efficiently handles both single-column and multi-column USING clauses
- Creates deep copies of Var nodes to avoid sharing issues
- The result is guaranteed to be properly typed and coerced to boolean
- Essential for converting the more user-friendly USING syntax into the internal ON clause representation