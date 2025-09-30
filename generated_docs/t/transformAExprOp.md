# transformAExprOp

## Location
[src/backend/parser/parse_expr.c:923-1003](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L923-L1003)

## Overview
Transforms binary operator expressions (A_Expr) from the parse tree into executable expression nodes, handling special cases like NULL equality comparisons, row operations, and subquery operations.

## Definition
```c
static Node *transformAExprOp(ParseState *pstate, A_Expr *a)
```

## Detailed Description
The `transformAExprOp` function is responsible for transforming binary operator expressions during SQL parsing. It handles several important special cases:

1. **NULL Equality Transformation**: When `Transform_null_equals` is enabled, converts "foo = NULL" or "NULL = foo" expressions into "IS NULL" tests for compatibility with standards-broken products like Microsoft's implementations.

2. **Row-Subquery Operations**: Converts "row op subselect" expressions into ROWCOMPARE sublinks, transforming the sublink type and associating the row expression as the test expression.

3. **Row-Row Comparisons**: Handles "ROW() op ROW()" operations by delegating to specialized row comparison logic that can perform element-wise comparisons.

4. **Ordinary Scalar Operations**: For standard scalar operators, transforms both operands recursively and creates an operator expression using the PostgreSQL operator resolution system.

The function preserves location information for error reporting and maintains proper expression evaluation context throughout the transformation process.

## Parameters / Member Variables
- `pstate`: Parse state containing context information and transformation state
- `a`: The A_Expr node representing the binary operator expression to transform

## Dependencies
- Functions called/Symbols referenced:
  - [exprIsNullConstant](../e/exprIsNullConstant.md)
  - [transformExprRecurse](transformExprRecurse.md)
  - [make_row_comparison_op](../m/make_row_comparison_op.md)
  - [make_op](../m/make_op.md)
  - makeNode (for NullTest creation)
  - [A_Expr](../A/A_Expr.md), NullTest, RowExpr, SubLink, CaseTestExpr (node types)
  - IS_NULL, EXPR_SUBLINK, ROWCOMPARE_SUBLINK (constants)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
- This function is static and only used within the parse_expr.c module
- The NULL equality transformation is controlled by the `Transform_null_equals` global variable
- Handles complex expression types including subqueries and row constructs
- Part of PostgreSQL's expression transformation pipeline during query parsing
- CaseTestExpr nodes are exempt from NULL equality transformation to preserve CASE-WHEN semantics
- Row operations require special handling due to their multi-element nature
- Location information is preserved for all transformed expressions to support accurate error reporting

## Simplified Source

```c
static Node *
transformAExprOp(ParseState *pstate, A_Expr *a)
{
    Node *lexpr = a->lexpr;
    Node *rexpr = a->rexpr;
    Node *result;

    // Special case: Convert "foo = NULL" to "foo IS NULL" for compatibility
    if (Transform_null_equals &&
        list_length(a->name) == 1 &&
        strcmp(strVal(linitial(a->name)), "=") == 0 &&
        (exprIsNullConstant(lexpr) || exprIsNullConstant(rexpr)) &&
        (!IsA(lexpr, CaseTestExpr) && !IsA(rexpr, CaseTestExpr))) {

        NullTest *n = makeNode(NullTest);
        n->nulltesttype = IS_NULL;
        n->location = a->location;
        n->arg = (Expr *) (exprIsNullConstant(lexpr) ? rexpr : lexpr);

        result = transformExprRecurse(pstate, (Node *) n);
    }
    // Special case: "row op subselect" -> ROWCOMPARE sublink
    else if (lexpr && IsA(lexpr, RowExpr) &&
             rexpr && IsA(rexpr, SubLink) &&
             ((SubLink *) rexpr)->subLinkType == EXPR_SUBLINK) {

        SubLink *s = (SubLink *) rexpr;
        s->subLinkType = ROWCOMPARE_SUBLINK;
        s->testexpr = lexpr;
        s->operName = a->name;
        s->location = a->location;

        result = transformExprRecurse(pstate, (Node *) s);
    }
    // Special case: "ROW() op ROW()" comparison
    else if (lexpr && IsA(lexpr, RowExpr) &&
             rexpr && IsA(rexpr, RowExpr)) {

        lexpr = transformExprRecurse(pstate, lexpr);
        rexpr = transformExprRecurse(pstate, rexpr);

        result = make_row_comparison_op(pstate, a->name,
                                        castNode(RowExpr, lexpr)->args,
                                        castNode(RowExpr, rexpr)->args,
                                        a->location);
    }
    // Default case: ordinary scalar operator
    else {
        Node *last_srf = pstate->p_last_srf;

        lexpr = transformExprRecurse(pstate, lexpr);
        rexpr = transformExprRecurse(pstate, rexpr);

        result = make_op(pstate, a->name, lexpr, rexpr, last_srf, a->location);
    }

    return result;
}
```