# make_row_comparison_op

## Location
[src/backend/parser/parse_expr.c:2816-3017](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L2816-L3017)

## Overview
Transforms a "row compare-op row" construct by analyzing operator semantics and creating appropriate comparison expressions for multi-column row comparisons.

## Definition
```c
static Node *make_row_comparison_op(ParseState *pstate, List *opname, List *largs, List *rargs, int location)
```

## Detailed Description
The `make_row_comparison_op` function handles row comparison operations where two row expressions are compared using operators like =, <>, <, <=, >, or >=. It takes lists of already-transformed expressions from both sides of the comparison and determines the appropriate comparison semantics. The function first validates that both row expressions have equal length and creates pairwise operator expressions using `make_op`. For equality (=) and inequality (<>) operations, it combines the pairwise operators with AND or OR respectively. For ordering operators (<, <=, >, >=), it analyzes btree operator families to determine the correct interpretation and creates a RowCompareExpr node. The function ensures all operators return boolean values and validates that operators have consistent btree semantics across all column pairs.

## Parameters / Member Variables
- `pstate`: ParseState pointer for parsing context, may be NULL for special cases
- `opname`: List containing the operator name to be applied 
- `largs`: List of already-transformed expressions from the left side of the comparison
- `rargs`: List of already-transformed expressions from the right side of the comparison
- `location`: Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [make_op](make_op.md)
  - castNode
  - [expression_returns_set](../e/expression_returns_set.md)
  - [get_op_btree_interpretation](../g/get_op_btree_interpretation.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_int_members](../b/bms_int_members.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [makeBoolExpr](makeBoolExpr.md)
  - [lappend_oid](../l/lappend_oid.md)
  - makeNode
- Called from (representative examples):
  - [transformAExprOp](../t/transformAExprOp.md)
  - [transformAExprIn](../t/transformAExprIn.md)
  - [transformSubLink](../t/transformSubLink.md)

## Notes and Other Information
- Returns different node types based on the operation: single OpExpr for single columns, BoolExpr (AND/OR) for equality/inequality, or RowCompareExpr for ordering comparisons
- Validates that row expressions have equal length and non-zero length
- Requires operators to return boolean type directly, not via coercion
- Analyzes btree operator families to determine consistent comparison semantics
- For ambiguous operator interpretations, arbitrarily selects the lowest strategy number
- Handles operator coercions that may be inserted by make_op by reconstructing argument lists
- The function guarantees that the output always returns boolean type

## Simplified Source

```c
static Node *
make_row_comparison_op(ParseState *pstate, List *opname,
                       List *largs, List *rargs, int location)
{
    RowCompareExpr *rcexpr;
    RowCompareType rctype;
    List *opexprs;
    List *opnos;
    List *opfamilies;
    ListCell *l, *r;
    int nopers;

    nopers = list_length(largs);

    // Validate equal length and non-zero length
    if (nopers != list_length(rargs))
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("unequal number of entries in row expressions"),
                       parser_errposition(pstate, location)));

    if (nopers == 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("cannot compare rows of zero length"),
                       parser_errposition(pstate, location)));

    // Create pairwise operator expressions
    opexprs = NIL;
    forboth(l, largs, r, rargs)
    {
        Node *larg = (Node *) lfirst(l);
        Node *rarg = (Node *) lfirst(r);
        OpExpr *cmp;

        cmp = castNode(OpExpr, make_op(pstate, opname, larg, rarg,
                                      pstate->p_last_srf, location));

        // Validate operator returns boolean
        if (cmp->opresulttype != BOOLOID)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("row comparison operator must yield type boolean, not type %s",
                                 format_type_be(cmp->opresulttype)),
                           parser_errposition(pstate, location)));

        opexprs = lappend(opexprs, cmp);
    }

    // Single column: return the operator directly
    if (nopers == 1)
        return (Node *) linitial(opexprs);

    // Determine row comparison semantics from btree operator families
    Bitmapset *strats = NULL;
    List **opinfo_lists = (List **) palloc(nopers * sizeof(List *));

    // Find common btree interpretations across all operators
    int i = 0;
    foreach(l, opexprs)
    {
        Oid opno = ((OpExpr *) lfirst(l))->opno;
        Bitmapset *this_strats = NULL;
        ListCell *j;

        opinfo_lists[i] = get_op_btree_interpretation(opno);

        foreach(j, opinfo_lists[i])
        {
            OpBtreeInterpretation *opinfo = lfirst(j);
            this_strats = bms_add_member(this_strats, opinfo->strategy);
        }

        if (i == 0)
            strats = this_strats;
        else
            strats = bms_int_members(strats, this_strats);
        i++;
    }

    // Pick the lowest common strategy number
    i = bms_next_member(strats, -1);
    if (i < 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("could not determine interpretation of row comparison operator %s",
                             strVal(llast(opname))),
                       parser_errposition(pstate, location)));

    rctype = (RowCompareType) i;

    // For equality/inequality, combine with AND/OR
    if (rctype == ROWCOMPARE_EQ)
        return (Node *) makeBoolExpr(AND_EXPR, opexprs, location);
    if (rctype == ROWCOMPARE_NE)
        return (Node *) makeBoolExpr(OR_EXPR, opexprs, location);

    // For ordering operators, create RowCompareExpr
    opfamilies = NIL;
    for (i = 0; i < nopers; i++)
    {
        Oid opfamily = InvalidOid;
        ListCell *j;

        foreach(j, opinfo_lists[i])
        {
            OpBtreeInterpretation *opinfo = lfirst(j);
            if (opinfo->strategy == rctype)
            {
                opfamily = opinfo->opfamily_id;
                break;
            }
        }
        opfamilies = lappend_oid(opfamilies, opfamily);
    }

    // Build final RowCompareExpr
    opnos = NIL;
    largs = NIL;
    rargs = NIL;
    foreach(l, opexprs)
    {
        OpExpr *cmp = (OpExpr *) lfirst(l);
        opnos = lappend_oid(opnos, cmp->opno);
        largs = lappend(largs, linitial(cmp->args));
        rargs = lappend(rargs, lsecond(cmp->args));
    }

    rcexpr = makeNode(RowCompareExpr);
    rcexpr->rctype = rctype;
    rcexpr->opnos = opnos;
    rcexpr->opfamilies = opfamilies;
    rcexpr->inputcollids = NIL;
    rcexpr->largs = largs;
    rcexpr->rargs = rargs;

    return (Node *) rcexpr;
}
```