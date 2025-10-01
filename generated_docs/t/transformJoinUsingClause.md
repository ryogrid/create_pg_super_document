# transformJoinUsingClause

## Location
[src/backend/parser/parse_clause.c:308-366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L308-L366)

## Overview
Builds a complete ON clause from partially-transformed USING lists by creating equality conditions between corresponding left and right join columns.

## Definition

```c
static Node *
transformJoinUsingClause(ParseState *pstate,
						 List *leftVars, List *rightVars)
```
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
  - [makeSimpleA_Expr](../m/makeSimpleA_Expr.md)
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

## Simplified Source

```c
static Node *transformJoinUsingClause(ParseState *pstate, List *leftVars, List *rightVars)
{
    Node *result;
    List *andargs = NIL;

    // Create equality conditions for each corresponding column pair
    forboth(lvars, leftVars, rvars, rightVars)
    {
        Var *lvar = (Var *) lfirst(lvars);
        Var *rvar = (Var *) lfirst(rvars);
        A_Expr *e;

        // Mark variables as needing SELECT privilege
        markVarForSelectPriv(pstate, lvar);
        markVarForSelectPriv(pstate, rvar);

        // Create lvar = rvar equality expression
        e = makeSimpleA_Expr(AEXPR_OP, "=",
                            (Node *) copyObject(lvar), (Node *) copyObject(rvar),
                            -1);

        andargs = lappend(andargs, e);
    }

    // Combine multiple conditions with AND, or use single condition
    if (list_length(andargs) == 1)
        result = (Node *) linitial(andargs);
    else
        result = (Node *) makeBoolExpr(AND_EXPR, andargs, -1);

    // Transform operators and ensure boolean type
    result = transformExpr(pstate, result, EXPR_KIND_JOIN_USING);
    result = coerce_to_boolean(pstate, result, "JOIN/USING");

    return result;
}
```