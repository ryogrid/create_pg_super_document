# transformTargetList

## Location
[src/backend/parser/parse_target.c:121-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L121-L219)

## Overview
Transforms a list of ResTarget nodes into a list of TargetEntry nodes, handling star expansion and multiassign expressions for SELECT, UPDATE, and RETURNING clauses.

## Definition
```c
List *
transformTargetList(ParseState *pstate, List *targetlist,
                    ParseExprKind exprKind)
```

## Detailed Description
This function is responsible for converting a list of ResTarget parse tree nodes into TargetEntry structures that represent the target list of a query. It handles the expansion of "something.*" expressions into multiple target entries when appropriate (in SELECT and RETURNING contexts, but not UPDATE). The function processes each ResTarget in the input list, either expanding star expressions or transforming individual expressions using transformTargetEntry. It also manages multiassign expressions that may be created during UPDATE statement processing.

## Parameters / Member Variables
- `pstate`: ParseState structure containing parser state and context information
- `targetlist`: List of ResTarget nodes to be transformed
- `exprKind`: Expression kind constant indicating the context (SELECT, UPDATE, RETURNING)

## Dependencies
- Functions called/Symbols referenced:
  - [transformTargetEntry](transformTargetEntry.md)
  - [ExpandColumnRefStar](../E/ExpandColumnRefStar.md)
  - [ExpandIndirectionStar](../E/ExpandIndirectionStar.md)
  - [list_concat](../l/list_concat.md)
  - [lappend](../l/lappend.md)
  - llast
  - IsA (macro)
  - lfirst (macro)
  - Assert (macro)
  - [ResTarget](../R/ResTarget.md)
  - [ColumnRef](../C/ColumnRef.md)
  - [A_Indirection](../A/A_Indirection.md)
  - [A_Star](../A/A_Star.md)
  - EXPR_KIND_UPDATE_SOURCE
- Called from (representative examples):
  - [transformSelectStmt](transformSelectStmt.md)
  - [transformUpdateTargetList](transformUpdateTargetList.md)
  - [transformReturningList](transformReturningList.md)
  - [transformPLAssignStmt](transformPLAssignStmt.md)

## Notes and Other Information
- Handles star expansion differently based on expression context (enabled for SELECT/RETURNING, disabled for UPDATE)
- Manages multiassign expressions that are created during UPDATE processing and attaches them to the end of the target list
- The function preserves the order of target entries while expanding star expressions inline
- Star expressions can appear in ColumnRef nodes or as indirection items in A_Indirection nodes
- Multiassign resjunk items are only expected in UPDATE contexts and their resource numbers are set later by transformUpdateStmt

## Simplified Source

```c
List *
transformTargetList(ParseState *pstate, List *targetlist,
                    ParseExprKind exprKind)
{
    List *p_target = NIL;
    bool expand_star;
    ListCell *o_target;

    Assert(pstate->p_multiassign_exprs == NIL);

    // Expand "something.*" in SELECT and RETURNING, but not UPDATE
    expand_star = (exprKind != EXPR_KIND_UPDATE_SOURCE);

    foreach(o_target, targetlist) {
        ResTarget *res = (ResTarget *) lfirst(o_target);

        // Check for "something.*" - could be in ColumnRef or A_Indirection
        if (expand_star) {
            if (IsA(res->val, ColumnRef)) {
                ColumnRef *cref = (ColumnRef *) res->val;

                if (IsA(llast(cref->fields), A_Star)) {
                    // It is something.*, expand into multiple items
                    p_target = list_concat(p_target,
                                           ExpandColumnRefStar(pstate, cref, true));
                    continue;
                }
            }
            else if (IsA(res->val, A_Indirection)) {
                A_Indirection *ind = (A_Indirection *) res->val;

                if (IsA(llast(ind->indirection), A_Star)) {
                    // It is something.*, expand into multiple items
                    p_target = list_concat(p_target,
                                           ExpandIndirectionStar(pstate, ind, true, exprKind));
                    continue;
                }
            }
        }

        // Not "something.*", transform as a single expression
        p_target = lappend(p_target,
                           transformTargetEntry(pstate,
                                                res->val,
                                                NULL,
                                                exprKind,
                                                res->name,
                                                false));
    }

    // Attach any multiassign resjunk items to the end (UPDATE only)
    if (pstate->p_multiassign_exprs) {
        Assert(exprKind == EXPR_KIND_UPDATE_SOURCE);
        p_target = list_concat(p_target, pstate->p_multiassign_exprs);
        pstate->p_multiassign_exprs = NIL;
    }

    return p_target;
}
```