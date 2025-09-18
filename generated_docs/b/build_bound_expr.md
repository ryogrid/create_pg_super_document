# build_bound_expr

## Location
src/backend/utils/adt/rangetypes.c: 2908 - 2953

## Overview
A static helper function that constructs comparison expressions between an element and a range boundary value using appropriate comparison operators.

## Definition


## Detailed Description
This function is a helper for range containment optimization that builds specific comparison expressions (OpExpr nodes) for range boundary checks. It determines the appropriate comparison operator based on whether the boundary is a lower or upper bound and whether it's inclusive or exclusive. The function uses the B-tree strategy numbers to select the correct operator from the specified operator family, creates a constant expression from the boundary value, and constructs the final comparison expression. This enables range containment operations to be transformed into simpler boundary comparisons for query optimization.

## Parameters / Member Variables
- : Expression representing the element to compare against the boundary
- : Datum value representing the range boundary
- : True if this is a lower bound, false for upper bound
- : True if the boundary is inclusive, false for exclusive
- : Type cache entry containing type information for the boundary value
- : Operator family to use for finding the comparison operator
- : Collation to use for the comparison operation

## Dependencies
- Functions called/Symbols referenced:
  - BTGreaterEqualStrategyNumber
  - BTGreaterStrategyNumber
  - BTLessEqualStrategyNumber
  - BTLessStrategyNumber
  - [get_opfamily_member](../g/get_opfamily_member.md)
  - [makeConst](../m/makeConst.md)
  - make_opclause
  - OidIsValid
  - BOOLOID
  - InvalidOid
- Called from (representative examples):
  - [find_simplified_clause](../f/find_simplified_clause.md)

## Notes and Other Information
This function maps boundary conditions to B-tree strategy numbers: lower inclusive uses >=, lower exclusive uses >, upper inclusive uses <=, and upper exclusive uses <. It creates a properly typed constant expression from the boundary datum and constructs an OpExpr that can be evaluated by the query executor. The function returns NULL if it cannot find an appropriate operator in the specified operator family, which causes the calling optimization to fall back to the original range containment operation.