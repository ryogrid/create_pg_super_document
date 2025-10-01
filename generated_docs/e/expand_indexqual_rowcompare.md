# expand_indexqual_rowcompare

## Location
[src/backend/optimizer/path/indxpath.c:2798-3019](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L2798-L3019)

## Overview
Constructs detailed index scan conditions from RowCompareExpr clauses by analyzing additional columns beyond the first one and building optimized index qualifications.

## Definition
```c
static IndexClause *
expand_indexqual_rowcompare(PlannerInfo *root,
                           RestrictInfo *rinfo,
                           int indexcol,
                           IndexOptInfo *index,
                           Oid expr_op,
                           bool var_on_left)
```

## Detailed Description
This function represents the detailed implementation phase of row comparison optimization, called after match_rowcompare_to_indexcol() has determined that a RowCompareExpr can potentially use an index. It analyzes all columns in the row comparison to determine how many can be effectively used as index qualifications.

The function examines each column pair in the row comparison, checking if additional columns match index columns with compatible operators and strategies. When all columns match perfectly, it uses the original clause as-is. When only some columns match, it constructs a shortened RowCompareExpr or a simple OpExpr, potentially converting strict inequalities (< or >) to non-strict ones (<= or >=) to ensure all matching rows are found. This transformation makes the condition lossy but allows for more efficient index scans.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context
- `rinfo`: RestrictInfo containing the RowCompareExpr clause to be expanded
- `indexcol`: Starting column number within the index (first column that matched)
- `index`: IndexOptInfo structure with metadata about the target index
- `expr_op`: Operator OID for the first column comparison
- `var_on_left`: Boolean indicating whether indexed columns are on the left side of comparisons

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [get_op_opfamily_properties](../g/get_op_opfamily_properties.md)
  - list_make1_int
  - list_make1_oid
  - [list_nth](../l/list_nth.md)
  - [list_nth_oid](../l/list_nth_oid.md)
  - [get_commutator](../g/get_commutator.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [pull_varnos](../p/pull_varnos.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [match_index_to_operand](../m/match_index_to_operand.md)
  - [get_op_opfamily_strategy](../g/get_op_opfamily_strategy.md)
  - IndexCollMatchesExprColl
  - [lappend_int](../l/lappend_int.md)
  - [lappend_oid](../l/lappend_oid.md)
  - [list_truncate](../l/list_truncate.md)
  - [get_opfamily_member](../g/get_opfamily_member.md)
  - [list_copy_head](../l/list_copy_head.md)
  - make_simple_restrictinfo
  - [make_opclause](../m/make_opclause.md)
  - copyObject
- Called from (representative examples):
  - [match_rowcompare_to_indexcol](../m/match_rowcompare_to_indexcol.md)

## Notes and Other Information
- Performs detailed analysis of multi-column row comparisons for index optimization
- Tracks which specific index columns are used via the indexcols list
- Handles operator commutation when indexed columns are on the right side
- Converts strict inequalities to non-strict ones when building lossy conditions
- Creates shortened RowCompareExpr for partial matches or simple OpExpr for single column matches
- Sets the lossy flag when not all columns in the original comparison can be used
- Ensures all operators use the same strategy (all <, all <=, etc.) for consistency
- Critical for optimizing complex multi-column WHERE clauses and achieving efficient index scans
- Works exclusively with B-tree indexes due to their support for ordered comparisons

## Simplified Source

```c
static IndexClause *
expand_indexqual_rowcompare(PlannerInfo *root,
                           RestrictInfo *rinfo,
                           int indexcol,
                           IndexOptInfo *index,
                           Oid expr_op,
                           bool var_on_left)
{
    IndexClause *iclause = makeNode(IndexClause);
    RowCompareExpr *clause = (RowCompareExpr *) rinfo->clause;
    int op_strategy;
    Oid op_lefttype, op_righttype;
    int matching_cols;
    List *expr_ops, *opfamilies, *lefttypes, *righttypes, *new_ops;
    List *var_args, *non_var_args;

    iclause->rinfo = rinfo;
    iclause->indexcol = indexcol;

    // Determine which side contains index variables
    if (var_on_left) {
        var_args = clause->largs;
        non_var_args = clause->rargs;
    } else {
        var_args = clause->rargs;
        non_var_args = clause->largs;
    }

    // Get properties of the first column's operator
    get_op_opfamily_properties(expr_op, index->opfamily[indexcol], false,
                              &op_strategy, &op_lefttype, &op_righttype);

    // Initialize lists for operator analysis
    iclause->indexcols = list_make1_int(indexcol);
    expr_ops = list_make1_oid(expr_op);
    opfamilies = list_make1_oid(index->opfamily[indexcol]);
    lefttypes = list_make1_oid(op_lefttype);
    righttypes = list_make1_oid(op_righttype);

    // Find additional matching columns
    matching_cols = 1;
    while (matching_cols < list_length(var_args)) {
        Node *varop = (Node *) list_nth(var_args, matching_cols);
        Node *constop = (Node *) list_nth(non_var_args, matching_cols);
        int i;

        expr_op = list_nth_oid(clause->opnos, matching_cols);
        if (!var_on_left) {
            expr_op = get_commutator(expr_op);
            if (expr_op == InvalidOid) break;
        }

        // Skip if constant side references indexed relation or is volatile
        if (bms_is_member(index->rel->relid, pull_varnos(root, constop)) ||
            contain_volatile_functions(constop))
            break;

        // Find matching index column
        for (i = 0; i < index->nkeycolumns; i++) {
            if (match_index_to_operand(varop, i, index) &&
                get_op_opfamily_strategy(expr_op, index->opfamily[i]) == op_strategy &&
                IndexCollMatchesExprColl(index->indexcollations[i],
                                       list_nth_oid(clause->inputcollids, matching_cols)))
                break;
        }
        if (i >= index->nkeycolumns) break;

        // Add matching column information
        iclause->indexcols = lappend_int(iclause->indexcols, i);
        get_op_opfamily_properties(expr_op, index->opfamily[i], false,
                                  &op_strategy, &op_lefttype, &op_righttype);
        expr_ops = lappend_oid(expr_ops, expr_op);
        opfamilies = lappend_oid(opfamilies, index->opfamily[i]);
        lefttypes = lappend_oid(lefttypes, op_lefttype);
        righttypes = lappend_oid(righttypes, op_righttype);
        matching_cols++;
    }

    // Determine if result is lossy (not all columns usable)
    iclause->lossy = (matching_cols != list_length(clause->opnos));

    // Build index qualification
    if (var_on_left && !iclause->lossy) {
        // Use original clause as-is
        iclause->indexquals = list_make1(rinfo);
    } else {
        // Need to modify the clause
        if (!iclause->lossy) {
            new_ops = expr_ops;
        } else if (op_strategy == BTLessEqualStrategyNumber ||
                  op_strategy == BTGreaterEqualStrategyNumber) {
            new_ops = list_truncate(expr_ops, matching_cols);
        } else {
            // Convert < to <= or > to >=
            if (op_strategy == BTLessStrategyNumber)
                op_strategy = BTLessEqualStrategyNumber;
            else if (op_strategy == BTGreaterStrategyNumber)
                op_strategy = BTGreaterEqualStrategyNumber;

            // Build new operator list
            new_ops = NIL;
            // ... (operator lookup logic simplified)
        }

        if (matching_cols > 1) {
            // Create shortened RowCompareExpr
            RowCompareExpr *rc = makeNode(RowCompareExpr);
            rc->rctype = (RowCompareType) op_strategy;
            rc->opnos = new_ops;
            rc->opfamilies = list_copy_head(clause->opfamilies, matching_cols);
            rc->inputcollids = list_copy_head(clause->inputcollids, matching_cols);
            rc->largs = list_copy_head(var_args, matching_cols);
            rc->rargs = list_copy_head(non_var_args, matching_cols);
            iclause->indexquals = list_make1(make_simple_restrictinfo(root, (Expr *) rc));
        } else {
            // Create simple OpExpr
            iclause->indexcols = NIL;
            Expr *op = make_opclause(linitial_oid(new_ops), BOOLOID, false,
                                   copyObject(linitial(var_args)),
                                   copyObject(linitial(non_var_args)),
                                   InvalidOid, linitial_oid(clause->inputcollids));
            iclause->indexquals = list_make1(make_simple_restrictinfo(root, op));
        }
    }

    return iclause;
}
```