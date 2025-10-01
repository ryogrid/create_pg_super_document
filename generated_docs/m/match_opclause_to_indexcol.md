# match_opclause_to_indexcol

## Location
[src/backend/optimizer/path/indxpath.c:2392-2510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L2392-L2510)

## Overview
Handles OpExpr (operator expression) cases for index clause matching, determining if binary operator clauses can be used with a specific index column.

## Definition
```c
static IndexClause *
match_opclause_to_indexcol(PlannerInfo *root,
                           RestrictInfo *rinfo,
                           int indexcol,
                           IndexOptInfo *index)
```

## Detailed Description
This function processes binary operator expressions to determine their compatibility with index columns. It handles two primary patterns:

1. **Left index pattern**: `(indexkey operator constant)` - Direct index usage
2. **Right index pattern**: `(constant operator indexkey)` - Requires operator commutation

The function performs comprehensive validation including:

- **Binary operator verification**: Only processes expressions with exactly two operands
- **Index key matching**: Uses `match_index_to_operand` to verify operand corresponds to index column
- **Volatility checking**: Ensures non-index operands don't contain volatile functions using `contain_volatile_functions`
- **Relation membership**: Confirms non-index operands don't reference the indexed relation
- **Operator family membership**: Validates operators belong to index's operator family via `op_in_opfamily`
- **Collation compatibility**: Matches expression and index collations using `IndexCollMatchesExprColl`

For right-index patterns, the function attempts operator commutation using `get_commutator` and `commute_restrictinfo` to transform the clause into executable form.

When standard operator matching fails, the function falls back to planner support functions via `get_index_clause_from_support` for advanced indexing strategies.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and cost information
- `rinfo`: RestrictInfo node wrapping the OpExpr clause to be tested
- `indexcol`: Zero-based column number within the target index
- `index`: IndexOptInfo structure containing index metadata and operator families

## Dependencies
- Functions called/Symbols referenced:
  - [match_index_to_operand](match_index_to_operand.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - IndexCollMatchesExprColl
  - [op_in_opfamily](../o/op_in_opfamily.md)
  - [get_commutator](../g/get_commutator.md)
  - [commute_restrictinfo](../c/commute_restrictinfo.md)
  - [set_opfuncid](../s/set_opfuncid.md)
  - [get_index_clause_from_support](../g/get_index_clause_from_support.md)
  - linitial/lsecond (list access)
  - makeNode (IndexClause creation)
- Called from (representative examples):
  - ec_member_matches_arg
  - [match_clause_to_indexcol](match_clause_to_indexcol.md)

## Notes and Other Information
- Only processes binary operators (expressions with exactly 2 arguments)
- Supports operator commutation to handle `constant op indexkey` patterns
- Falls back to planner support functions for complex indexing scenarios
- Performs strict validation on volatility, relation membership, and collation compatibility
- Part of the comprehensive index optimization system in PostgreSQL's query planner
- Located in `src/backend/optimizer/path/indxpath.c:2392-2510`
- Returns non-lossy IndexClause nodes for standard operator matching cases

## Simplified Source

```c
static IndexClause *
match_opclause_to_indexcol(PlannerInfo *root,
                           RestrictInfo *rinfo,
                           int indexcol,
                           IndexOptInfo *index)
{
    IndexClause *iclause;
    OpExpr *clause = (OpExpr *) rinfo->clause;
    Node *leftop, *rightop;
    Oid expr_op, expr_coll;
    Index index_relid = index->rel->relid;
    Oid opfamily = index->opfamily[indexcol];
    Oid idxcollation = index->indexcollations[indexcol];

    // Only binary operators are supported
    if (list_length(clause->args) != 2)
        return NULL;

    leftop = (Node *) linitial(clause->args);
    rightop = (Node *) lsecond(clause->args);
    expr_op = clause->opno;
    expr_coll = clause->inputcollid;

    // Case 1: (indexkey operator constant)
    if (match_index_to_operand(leftop, indexcol, index) &&
        !bms_is_member(index_relid, rinfo->right_relids) &&
        !contain_volatile_functions(rightop))
    {
        // Try direct operator family membership
        if (IndexCollMatchesExprColl(idxcollation, expr_coll) &&
            op_in_opfamily(expr_op, opfamily))
        {
            iclause = makeNode(IndexClause);
            iclause->rinfo = rinfo;
            iclause->indexquals = list_make1(rinfo);
            iclause->lossy = false;
            iclause->indexcol = indexcol;
            iclause->indexcols = NIL;
            return iclause;
        }

        // Fallback to support function
        set_opfuncid(clause);
        return get_index_clause_from_support(root, rinfo, clause->opfuncid,
                                            0, indexcol, index);
    }

    // Case 2: (constant operator indexkey) - needs commutation
    if (match_index_to_operand(rightop, indexcol, index) &&
        !bms_is_member(index_relid, rinfo->left_relids) &&
        !contain_volatile_functions(leftop))
    {
        if (IndexCollMatchesExprColl(idxcollation, expr_coll))
        {
            Oid comm_op = get_commutator(expr_op);

            if (OidIsValid(comm_op) && op_in_opfamily(comm_op, opfamily))
            {
                RestrictInfo *commrinfo = commute_restrictinfo(rinfo, comm_op);

                iclause = makeNode(IndexClause);
                iclause->rinfo = rinfo;
                iclause->indexquals = list_make1(commrinfo);
                iclause->lossy = false;
                iclause->indexcol = indexcol;
                iclause->indexcols = NIL;
                return iclause;
            }
        }

        // Fallback to support function
        set_opfuncid(clause);
        return get_index_clause_from_support(root, rinfo, clause->opfuncid,
                                            1, indexcol, index);
    }

    return NULL;
}
```