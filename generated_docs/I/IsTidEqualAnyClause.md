# IsTidEqualAnyClause

## Location
[src/backend/optimizer/path/tidpath.c:172-210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/tidpath.c#L172-L210)

## Overview
IsTidEqualAnyClause is a static function that identifies whether a RestrictInfo represents a CTID equality clause using the ANY operator for matching against multiple values.

## Definition

```c
static bool
IsTidEqualAnyClause(PlannerInfo *root, RestrictInfo *rinfo, RelOptInfo *rel)
```
## Detailed Description
This function determines if a RestrictInfo represents a clause of the form "CTID = ANY (pseudoconstant_array)", where the CTID variable belongs to the specified relation and the array contains values that don't reference the relation. It validates that the clause is a ScalarArrayOpExpr using the TID equality operator with useOr=true, ensures the first argument is a CTID variable for the target relation, and verifies that the second argument (the array) is a pseudoconstant expression. This enables the optimizer to consider TID-based access when multiple specific tuple identifiers are being sought.

## Parameters / Member Variables
- : A PlannerInfo structure containing planner state and context
- : A RestrictInfo structure containing the clause to be examined
- : A RelOptInfo structure representing the relation being analyzed

## Dependencies
- Functions called/Symbols referenced:
  - IsA (type checking macro)
  - [ScalarArrayOpExpr](../S/ScalarArrayOpExpr.md) (scalar array operation expression type)
  - TIDEqualOperator (constant for TID equality operator)
  - [list_length](../l/list_length.md) (gets list length)
  - linitial (gets first list element)
  - lsecond (gets second list element)
  - [IsCTIDVar](IsCTIDVar.md) (checks if variable is CTID)
  - [bms_is_member](../b/bms_is_member.md) (checks bitmap membership)
  - [pull_varnos](../p/pull_varnos.md) (extracts variable numbers from expression)
  - [contain_volatile_functions](../c/contain_volatile_functions.md) (checks for volatile functions)
- Called from (representative examples):
  - [RestrictInfoIsTidQual](../R/RestrictInfoIsTidQual.md)

## Notes and Other Information
Unlike the other TID clause functions, this one handles the ANY operator which allows matching against multiple TID values in a single clause. It requires the useOr flag to be true (indicating OR semantics for the array elements) and specifically checks that the CTID variable is the first argument. The function uses pull_varnos to ensure the array expression doesn't reference the target relation, maintaining the pseudoconstant requirement essential for TID-based optimization.

## Simplified Source

```c
static bool
IsTidEqualAnyClause(PlannerInfo *root, RestrictInfo *rinfo, RelOptInfo *rel)
{
    ScalarArrayOpExpr *node;
    Node *arg1, *arg2;

    // Must be a ScalarArrayOpExpr
    if (!(rinfo->clause && IsA(rinfo->clause, ScalarArrayOpExpr)))
        return false;

    node = (ScalarArrayOpExpr *) rinfo->clause;

    // Must use TID equality operator with OR semantics
    if (node->opno != TIDEqualOperator || !node->useOr)
        return false;

    // Extract the two arguments
    arg1 = linitial(node->args);
    arg2 = lsecond(node->args);

    // First argument must be CTID variable for this relation
    if (arg1 && IsA(arg1, Var) && IsCTIDVar((Var *) arg1, rel))
    {
        // Second argument (array) must be a pseudoconstant
        if (bms_is_member(rel->relid, pull_varnos(root, arg2)) ||
            contain_volatile_functions(arg2))
            return false;

        return true;
    }

    return false;
}
```