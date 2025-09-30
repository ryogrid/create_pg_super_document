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
  - [MultiAssignRef](../M/MultiAssignRef.md), SubLink, RowExpr (struct types for multi-assignment references)
  - EXPR_KIND_UPDATE_SOURCE (expression context for UPDATE sources)
  - EXPR_SUBLINK, MULTIEXPR_SUBLINK (sublink type constants)
  - PARAM_MULTIEXPR (parameter type for multi-expression references)
  - [transformExprRecurse](transformExprRecurse.md) (recursively transforms expressions)
  - [transformRowExpr](transformRowExpr.md) (transforms row expressions with special handling)
  - [count_nonjunk_tlist_entries](../c/count_nonjunk_tlist_entries.md) (counts non-junk target list entries)
  - [makeTargetEntry](../m/makeTargetEntry.md), makeNode (node creation functions)
  - [exprType](../e/exprType.md), exprTypmod, exprCollation, exprLocation (expression metadata functions)
- Called from:
  - [transformExprRecurse](transformExprRecurse.md) (main expression transformation dispatcher)

## Notes and Other Information
- This function is specific to UPDATE statement processing and only operates in EXPR_KIND_UPDATE_SOURCE context
- Supports two source types: SubLinks (subqueries) and RowExprs (row constructors)
- Uses p_multiassign_exprs list to track transformed expressions across multiple column references
- Creates PARAM_MULTIEXPR parameters to reference subquery columns efficiently
- Validates that the number of source columns matches the number of target columns
- For RowExprs, cleans up the temporary storage when processing the last column
- The function is static, indicating it's only used within the parse_expr.c module
- Error handling includes syntax errors for column count mismatches and unsupported source types

## Simplified Source

```c
static Node *transformMultiAssignRef(ParseState *pstate, MultiAssignRef *maref) {
    // Ensure we're in UPDATE context
    Assert(pstate->p_expr_kind == EXPR_KIND_UPDATE_SOURCE);

    TargetEntry *tle;

    // First column: transform the source expression
    if (maref->colno == 1) {
        if (IsA(maref->source, SubLink) &&
            ((SubLink *) maref->source)->subLinkType == EXPR_SUBLINK) {

            // Handle subquery source
            SubLink *sublink = (SubLink *) maref->source;
            sublink->subLinkType = MULTIEXPR_SUBLINK;
            sublink = (SubLink *) transformExprRecurse(pstate, (Node *) sublink);

            Query *qtree = castNode(Query, sublink->subselect);

            // Validate column count
            if (count_nonjunk_tlist_entries(qtree->targetList) != maref->ncolumns) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                               errmsg("number of columns does not match number of values"),
                               parser_errposition(pstate, sublink->location)));
            }

            // Store in multiassign list and assign unique ID
            tle = makeTargetEntry((Expr *) sublink, 0, NULL, true);
            pstate->p_multiassign_exprs = lappend(pstate->p_multiassign_exprs, tle);
            sublink->subLinkId = list_length(pstate->p_multiassign_exprs);

        } else if (IsA(maref->source, RowExpr)) {
            // Handle row expression source
            RowExpr *rexpr = (RowExpr *) transformRowExpr(pstate,
                                                         (RowExpr *) maref->source, true);

            // Validate column count
            if (list_length(rexpr->args) != maref->ncolumns) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                               errmsg("number of columns does not match number of values"),
                               parser_errposition(pstate, rexpr->location)));
            }

            // Store temporarily in multiassign list
            tle = makeTargetEntry((Expr *) rexpr, 0, NULL, true);
            pstate->p_multiassign_exprs = lappend(pstate->p_multiassign_exprs, tle);

        } else {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression"),
                           parser_errposition(pstate, exprLocation(maref->source))));
        }
    } else {
        // Subsequent columns: retrieve from stored expression
        Assert(pstate->p_multiassign_exprs != NIL);
        tle = (TargetEntry *) llast(pstate->p_multiassign_exprs);
    }

    // Generate appropriate output expression
    if (IsA(tle->expr, SubLink)) {
        // Create parameter for subquery column
        SubLink *sublink = (SubLink *) tle->expr;
        Query *qtree = castNode(Query, sublink->subselect);
        TargetEntry *target_tle = (TargetEntry *) list_nth(qtree->targetList, maref->colno - 1);

        Param *param = makeNode(Param);
        param->paramkind = PARAM_MULTIEXPR;
        param->paramid = (sublink->subLinkId << 16) | maref->colno;
        param->paramtype = exprType((Node *) target_tle->expr);
        param->paramtypmod = exprTypmod((Node *) target_tle->expr);
        param->paramcollid = exprCollation((Node *) target_tle->expr);
        param->location = exprLocation((Node *) target_tle->expr);

        return (Node *) param;
    }

    if (IsA(tle->expr, RowExpr)) {
        // Extract element from row expression
        RowExpr *rexpr = (RowExpr *) tle->expr;
        Node *result = (Node *) list_nth(rexpr->args, maref->colno - 1);

        // Clean up on last column
        if (maref->colno == maref->ncolumns) {
            pstate->p_multiassign_exprs = list_delete_last(pstate->p_multiassign_exprs);
        }

        return result;
    }

    elog(ERROR, "unexpected expr type in multiassign list");
    return NULL;
}
```