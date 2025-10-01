# match_clause_to_ordering_op

## Location
[src/backend/optimizer/path/indxpath.c:3130-3243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L3130-L3243)

## Overview
Determines whether an ordering operator expression matches an index column for query optimization purposes.

## Definition

```c
static Expr *
match_clause_to_ordering_op(IndexOptInfo *index,
							int indexcol,
							Expr *clause,
							Oid pk_opfamily)
```
## Detailed Description
This function is a key component of PostgreSQL's query optimizer that determines if a given ordering expression can be satisfied by an index column. It's simpler than  as it only handles simple OpExpr cases. The function checks if the input expression is of the form  or  where the operator is an ordering operator for the column's opfamily.

The function validates collation compatibility, checks for proper operand structure, and ensures the operator yields the correct sorting semantics. If a match is found but requires commutation (when the indexkey is on the right side), it creates and returns a commuted version of the clause.

## Parameters / Member Variables
- : The IndexOptInfo structure representing the index of interest
- : Column number of the index (counting from 0) to match against
- : The ordering expression to be tested for compatibility
- : The btree opfamily describing the required sort order

## Dependencies
- Functions called/Symbols referenced:
  - [is_opclause](../i/is_opclause.md)
  - [get_leftop](../g/get_leftop.md)
  - [get_rightop](../g/get_rightop.md)
  - IndexCollMatchesExprColl
  - [match_index_to_operand](match_index_to_operand.md)
  - [contain_var_clause](../c/contain_var_clause.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [get_commutator](../g/get_commutator.md)
  - [get_op_opfamily_sortfamily](../g/get_op_opfamily_sortfamily.md)
  - list_make2
  - [OpExpr](../O/OpExpr.md) (structure)
  - [IndexOptInfo](../I/IndexOptInfo.md) (structure)
- Called from (representative examples):
  - ec_member_matches_arg
  - [match_pathkeys_to_index](match_pathkeys_to_index.md)

## Notes and Other Information
- Currently does not consider the collation of the ordering operator's result, focusing on input collation compatibility instead
- Returns the original clause if indexkey is on the left, or a commuted copy if indexkey is on the right
- Returns NULL if no match is found
- Only handles binary operator clauses and rejects volatile functions in operands
- File location: src/backend/optimizer/path/indxpath.c:3130-3243

## Simplified Source

```c
static Expr *
match_clause_to_ordering_op(IndexOptInfo *index,
                           int indexcol,
                           Expr *clause,
                           Oid pk_opfamily)
{
    Oid opfamily = index->opfamily[indexcol];
    Oid idxcollation = index->indexcollations[indexcol];
    Node *leftop, *rightop;
    Oid expr_op, expr_coll, sortfamily;
    bool commuted;

    // Must be a binary operator clause
    if (!is_opclause(clause))
        return NULL;

    leftop = get_leftop(clause);
    rightop = get_rightop(clause);
    if (!leftop || !rightop)
        return NULL;

    expr_op = ((OpExpr *) clause)->opno;
    expr_coll = ((OpExpr *) clause)->inputcollid;

    // Check collation compatibility
    if (!IndexCollMatchesExprColl(idxcollation, expr_coll))
        return NULL;

    // Check for (indexkey op constant) pattern
    if (match_index_to_operand(leftop, indexcol, index) &&
        !contain_var_clause(rightop) &&
        !contain_volatile_functions(rightop))
    {
        commuted = false;
    }
    // Check for (constant op indexkey) pattern
    else if (match_index_to_operand(rightop, indexcol, index) &&
             !contain_var_clause(leftop) &&
             !contain_volatile_functions(leftop))
    {
        // Need commuted operator
        expr_op = get_commutator(expr_op);
        if (expr_op == InvalidOid)
            return NULL;
        commuted = true;
    }
    else
        return NULL;

    // Verify operator produces correct sorting semantics
    sortfamily = get_op_opfamily_sortfamily(expr_op, opfamily);
    if (sortfamily != pk_opfamily)
        return NULL;

    // Return original or commuted clause
    if (commuted)
    {
        OpExpr *newclause = makeNode(OpExpr);
        memcpy(newclause, clause, sizeof(OpExpr));
        newclause->opno = expr_op;
        newclause->opfuncid = InvalidOid;
        newclause->args = list_make2(rightop, leftop);
        clause = (Expr *) newclause;
    }

    return clause;
}
```