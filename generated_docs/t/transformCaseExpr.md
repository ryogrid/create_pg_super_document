# transformCaseExpr

## Location
[src/backend/parser/parse_expr.c:1632-1771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L1632-L1771)

## Overview
Transforms a CASE expression node (CaseExpr) during parsing by handling both simple and searched CASE forms, processing WHEN clauses, and performing type resolution and coercion.

## Definition
```c
static Node *transformCaseExpr(ParseState *pstate, CaseExpr *c)
```

## Detailed Description
The transformCaseExpr function handles transformation of CASE expressions during SQL parsing, supporting both simple CASE (CASE expr WHEN value THEN result) and searched CASE (CASE WHEN condition THEN result) forms. The function performs comprehensive type checking, creates placeholder expressions for simple CASE forms, and ensures all result expressions have compatible types.

Key processing steps:
1. **Test expression handling**: Transforms the optional test expression, handling untyped literals and collation assignment
2. **Placeholder creation**: For simple CASE forms, creates a CaseTestExpr placeholder to represent the test value
3. **WHEN clause transformation**: Processes each WHEN clause, expanding simple CASE forms to equality comparisons
4. **Type resolution**: Determines a common result type for all WHEN and ELSE expressions
5. **Type coercion**: Coerces all result expressions to the common type
6. **SRF validation**: Ensures no set-returning functions are used within the CASE expression

The function handles both shorthand (simple) and full (searched) CASE syntax by internally converting simple CASE to searched CASE using equality comparisons.

## Parameters / Member Variables
- `pstate`: ParseState context containing parsing state information and error handling context
- `c`: CaseExpr node containing the test expression, WHEN clauses, default result, and location information

## Dependencies
- Functions called/Symbols referenced:
  - [CaseExpr](../C/CaseExpr.md), CaseWhen, CaseTestExpr (struct types for CASE expressions)
  - [A_Const](../A/A_Const.md) (struct type for constant values)
  - [transformExprRecurse](transformExprRecurse.md) (recursively transforms expression nodes)
  - [coerce_to_common_type](../c/coerce_to_common_type.md), coerce_to_boolean (type coercion functions)
  - [assign_expr_collations](../a/assign_expr_collations.md) (assigns collations to expressions)
  - [makeSimpleA_Expr](../m/makeSimpleA_Expr.md) (creates simple A_Expr nodes for equality)
  - [select_common_type](../s/select_common_type.md) (determines common type from expression list)
  - [exprType](../e/exprType.md), exprTypmod, exprCollation, exprLocation (expression metadata functions)
  - AEXPR_OP (A_Expr operation type constant)
- Called from:
  - [transformExprRecurse](transformExprRecurse.md) (main expression transformation dispatcher)

## Notes and Other Information
- This function is part of the SQL parser's expression transformation pipeline
- Supports both simple CASE (with test expression) and searched CASE (without test expression) forms
- For simple CASE, creates equality comparisons using makeSimpleA_Expr
- Handles untyped literals in test expressions by coercing to TEXT type
- Uses CaseTestExpr placeholders to avoid re-evaluating the test expression
- Performs type resolution to find a common type for all result expressions
- Validates that set-returning functions are not used within CASE expressions
- The default result is given priority in type resolution (though this is noted as potentially questionable behavior)
- The function is static, indicating it's only used within the parse_expr.c module

## Simplified Source

```c
static Node *
transformCaseExpr(ParseState *pstate, CaseExpr *c)
{
    CaseExpr *newc = makeNode(CaseExpr);
    Node *last_srf = pstate->p_last_srf;
    Node *arg;
    CaseTestExpr *placeholder;
    List *newargs;
    List *resultexprs;
    Node *defresult;
    Oid ptype;

    // Transform test expression and create placeholder if needed
    arg = transformExprRecurse(pstate, (Node *) c->arg);
    if (arg) {
        // Handle untyped literals by coercing to text
        if (exprType(arg) == UNKNOWNOID)
            arg = coerce_to_common_type(pstate, arg, TEXTOID, "CASE");

        assign_expr_collations(pstate, arg);

        // Create placeholder for simple CASE form
        placeholder = makeNode(CaseTestExpr);
        placeholder->typeId = exprType(arg);
        placeholder->typeMod = exprTypmod(arg);
        placeholder->collation = exprCollation(arg);
    } else {
        placeholder = NULL;
    }
    newc->arg = (Expr *) arg;

    // Transform WHEN clauses
    newargs = NIL;
    resultexprs = NIL;
    foreach(l, c->args) {
        CaseWhen *w = lfirst_node(CaseWhen, l);
        CaseWhen *neww = makeNode(CaseWhen);
        Node *warg = (Node *) w->expr;

        // For simple CASE, expand to equality comparison
        if (placeholder) {
            warg = (Node *) makeSimpleA_Expr(AEXPR_OP, "=",
                                             (Node *) placeholder,
                                             warg, w->location);
        }

        neww->expr = (Expr *) transformExprRecurse(pstate, warg);
        neww->expr = (Expr *) coerce_to_boolean(pstate, (Node *) neww->expr,
                                                 "CASE/WHEN");

        neww->result = (Expr *) transformExprRecurse(pstate, (Node *) w->result);
        neww->location = w->location;

        newargs = lappend(newargs, neww);
        resultexprs = lappend(resultexprs, neww->result);
    }
    newc->args = newargs;

    // Transform default clause (create NULL if missing)
    defresult = (Node *) c->defresult;
    if (defresult == NULL) {
        A_Const *n = makeNode(A_Const);
        n->isnull = true;
        n->location = -1;
        defresult = (Node *) n;
    }
    newc->defresult = (Expr *) transformExprRecurse(pstate, defresult);

    // Determine common result type
    resultexprs = lcons(newc->defresult, resultexprs);
    ptype = select_common_type(pstate, resultexprs, "CASE", NULL);
    newc->casetype = ptype;

    // Coerce all results to common type
    newc->defresult = (Expr *) coerce_to_common_type(pstate,
                                                      (Node *) newc->defresult,
                                                      ptype, "CASE/ELSE");

    foreach(l, newc->args) {
        CaseWhen *w = (CaseWhen *) lfirst(l);
        w->result = (Expr *) coerce_to_common_type(pstate,
                                                   (Node *) w->result,
                                                   ptype, "CASE/WHEN");
    }

    // Check for set-returning functions
    if (pstate->p_last_srf != last_srf)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-returning functions are not allowed in CASE"),
                        errhint("You might be able to move the set-returning function into a LATERAL FROM item."),
                        parser_errposition(pstate, exprLocation(pstate->p_last_srf))));

    newc->location = c->location;
    return (Node *) newc;
}
```