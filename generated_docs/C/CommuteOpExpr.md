# CommuteOpExpr

## Location
[src/backend/optimizer/util/clauses.c:2147-2185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L2147-L2185)

## Overview
Commutes (reverses the order of arguments) a binary operator expression by swapping its operands and replacing the operator with its commutator.

## Definition
```c
void CommuteOpExpr(OpExpr *clause)
```

## Detailed Description
This function performs in-place commutation of a binary operator expression, which involves:

1. Validating that the clause is a binary operator expression with exactly 2 arguments
2. Finding the commutator operator for the current operator using get_commutator()
3. Replacing the operator ID (opno) with its commutator
4. Invalidating the cached function ID (opfuncid) to force re-lookup
5. Swapping the two operand arguments

The function destructively modifies the original OpExpr clause rather than creating a new one. This is a performance optimization but means the caller must be aware that the original clause is altered.

Common use cases include query optimization scenarios where reordering operands can enable better index usage or join ordering (e.g., converting "constant = variable" to "variable = constant" for index scans).

## Parameters / Member Variables
- `clause`: The binary operator expression to commute (modified in-place)

## Dependencies
- Functions called/Symbols referenced:
  - [OpExpr](../O/OpExpr.md) (the operator expression type being manipulated)
  - [is_opclause](../i/is_opclause.md) (validates the clause is an operator expression)
  - [get_commutator](../g/get_commutator.md) (finds the commutator operator for the current operator)
  - lsecond (accesses the second element in the argument list)
- Called from (representative examples):
  - [get_switched_clauses](../g/get_switched_clauses.md) (during plan creation to generate alternative clause forms)

## Notes and Other Information
- **DESTRUCTIVE**: Modifies the original clause in-place rather than creating a copy
- Requires the operator to have a defined commutator; will error if no commutator exists
- Only works with binary operators (exactly 2 arguments)
- Does not modify opresulttype, opretset, opcollid, or inputcollid as these remain valid after commutation
- Invalidates opfuncid to force function lookup with the new operator
- Used primarily in query optimization for generating equivalent but potentially more efficient expressions