# IsBinaryTidClause

## Location
[src/backend/optimizer/path/tidpath.c:76-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/tidpath.c#L76-L129)

## Overview
IsBinaryTidClause is a static function that determines whether a RestrictInfo represents a binary operation involving a CTID variable and a pseudoconstant.

## Definition

```c
static bool
IsBinaryTidClause(RestrictInfo *rinfo, RelOptInfo *rel)
```
## Detailed Description
This function examines a RestrictInfo to check if it represents a clause of the form "CTID OP pseudoconstant" or "pseudoconstant OP CTID", where OP is a binary operation. It validates that one operand is a CTID variable belonging to the specified relation, while the other operand is a pseudoconstant (an expression that doesn't reference the relation and contains no volatile functions). This is essential for identifying clauses that can be used for TID-based access paths in query optimization.

## Parameters / Member Variables
- : A RestrictInfo structure containing the clause to be examined
- : A RelOptInfo structure representing the relation being analyzed

## Dependencies
- Functions called/Symbols referenced:
  - [is_opclause](../i/is_opclause.md) (checks if clause is an OpExpr)
  - [list_length](../l/list_length.md) (gets list length)
  - linitial (gets first list element)
  - lsecond (gets second list element)
  - IsA (type checking macro)
  - [IsCTIDVar](IsCTIDVar.md) (checks if variable is CTID)
  - [bms_is_member](../b/bms_is_member.md) (checks bitmap membership)
  - [contain_volatile_functions](../c/contain_volatile_functions.md) (checks for volatile functions)
- Called from (representative examples):
  - [IsTidEqualClause](IsTidEqualClause.md)
  - [IsTidRangeClause](IsTidRangeClause.md)

## Notes and Other Information
The function performs several validation steps: ensures the clause is an OpExpr with exactly two arguments, identifies which argument (if any) is a CTID variable for the specified relation, and verifies that the other argument is a true pseudoconstant by checking that it doesn't reference the relation and contains no volatile functions. This careful validation ensures that only appropriate clauses are considered for TID-based optimization.

## Simplified Source

```c
static bool
IsBinaryTidClause(RestrictInfo *rinfo, RelOptInfo *rel)
{
    OpExpr *node;
    Node *arg1, *arg2, *other;
    Relids other_relids;

    // Must be an OpExpr with exactly two arguments
    if (!is_opclause(rinfo->clause))
        return false;

    node = (OpExpr *) rinfo->clause;
    if (list_length(node->args) != 2)
        return false;

    arg1 = linitial(node->args);
    arg2 = lsecond(node->args);

    // Find which argument is CTID and which is the other operand
    other = NULL;
    other_relids = NULL;

    if (arg1 && IsA(arg1, Var) && IsCTIDVar((Var *) arg1, rel))
    {
        other = arg2;
        other_relids = rinfo->right_relids;
    }
    else if (arg2 && IsA(arg2, Var) && IsCTIDVar((Var *) arg2, rel))
    {
        other = arg1;
        other_relids = rinfo->left_relids;
    }

    if (!other)
        return false;

    // The other argument must be a pseudoconstant
    // (not reference this relation and not contain volatile functions)
    if (bms_is_member(rel->relid, other_relids) ||
        contain_volatile_functions(other))
        return false;

    return true;
}
```