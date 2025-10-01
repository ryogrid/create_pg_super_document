# match_saopclause_to_indexcol

## Location
[src/backend/optimizer/path/indxpath.c:2623-2690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L2623-L2690)

## Overview
Handles ScalarArrayOpExpr clauses (ANY/IN operations) to determine if they can be converted into index scan conditions for query optimization.

## Definition
```c
static IndexClause *
match_saopclause_to_indexcol(PlannerInfo *root,
                             RestrictInfo *rinfo,
                             int indexcol,
                             IndexOptInfo *index)
```

## Detailed Description
This function specializes in processing ScalarArrayOpExpr clauses, which represent SQL operations like "column = ANY(array)" or "column IN (value1, value2, ...)". It determines whether such expressions can be efficiently executed using an index scan rather than a sequential scan with post-filtering.

The function performs several validation checks: it only accepts ANY clauses (not ALL clauses), verifies that the left operand matches an indexed column, ensures the right operand is a pseudo-constant array that doesn't reference the indexed relation, and confirms the operator is compatible with the index's operator family. When all conditions are met, it creates an IndexClause that can be used for index scanning.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context
- `rinfo`: RestrictInfo containing the ScalarArrayOpExpr clause to be analyzed  
- `indexcol`: Column number within the index being considered
- `index`: IndexOptInfo structure with metadata about the target index

## Dependencies
- Functions called/Symbols referenced:
  - linitial
  - lsecond
  - [pull_varnos](../p/pull_varnos.md)
  - [match_index_to_operand](match_index_to_operand.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - IndexCollMatchesExprColl
  - [op_in_opfamily](../o/op_in_opfamily.md)
  - makeNode
  - list_make1
- Called from (representative examples):
  - [match_clause_to_indexcol](match_clause_to_indexcol.md)

## Notes and Other Information
- Only processes ANY clauses (useOr = true), rejecting ALL clauses which have different semantics
- Requires the left operand to match the indexed column and right operand to be a constant array
- Checks operator compatibility with the index's operator family and collation matching
- Creates non-lossy IndexClause when successful since array operations have exact semantics  
- Currently does not invoke planner support functions for ScalarArrayOpExpr, though this could be extended
- Essential for optimizing IN clauses and ANY operations against indexed columns

## Simplified Source

```c
static IndexClause *
match_saopclause_to_indexcol(PlannerInfo *root,
                             RestrictInfo *rinfo,
                             int indexcol,
                             IndexOptInfo *index)
{
    ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) rinfo->clause;
    Node *leftop, *rightop;
    Relids right_relids;
    Oid expr_op, expr_coll;
    Index index_relid = index->rel->relid;
    Oid opfamily = index->opfamily[indexcol];
    Oid idxcollation = index->indexcollations[indexcol];

    // Only accept ANY clauses, not ALL clauses
    if (!saop->useOr)
        return NULL;

    leftop = (Node *) linitial(saop->args);
    rightop = (Node *) lsecond(saop->args);
    right_relids = pull_varnos(root, rightop);
    expr_op = saop->opno;
    expr_coll = saop->inputcollid;

    // Must have: indexkey = ANY(constant_array)
    if (match_index_to_operand(leftop, indexcol, index) &&
        !bms_is_member(index_relid, right_relids) &&
        !contain_volatile_functions(rightop))
    {
        // Check operator compatibility
        if (IndexCollMatchesExprColl(idxcollation, expr_coll) &&
            op_in_opfamily(expr_op, opfamily))
        {
            IndexClause *iclause = makeNode(IndexClause);

            iclause->rinfo = rinfo;
            iclause->indexquals = list_make1(rinfo);
            iclause->lossy = false;
            iclause->indexcol = indexcol;
            iclause->indexcols = NIL;
            return iclause;
        }

        // Note: Currently no support function fallback for ScalarArrayOpExpr
    }

    return NULL;
}
```