# build_bound_expr

## Location
[src/backend/utils/adt/rangetypes.c:2908-2953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2908-L2953)

## Overview
A static helper function that constructs comparison expressions between an element and a range boundary value using appropriate comparison operators.

## Definition

```c
static Expr *
build_bound_expr(Expr *elemExpr, Datum val,
				 bool isLowerBound, bool isInclusive,
				 TypeCacheEntry *typeCache,
				 Oid opfamily, Oid rng_collation)
```
## Detailed Description
This function is a helper for range containment optimization that builds specific comparison expressions (OpExpr nodes) for range boundary checks. It determines the appropriate comparison operator based on whether the boundary is a lower or upper bound and whether it's inclusive or exclusive. The function uses the B-tree strategy numbers to select the correct operator from the specified operator family, creates a constant expression from the boundary value, and constructs the final comparison expression. This enables range containment operations to be transformed into simpler boundary comparisons for query optimization.

## Parameters / Member Variables
- `*elemExpr`: Expression representing the element to compare against the boundary
- `val`: Datum value representing the range boundary
- `isLowerBound`: True if this is a lower bound, false for upper bound
- `isInclusive`: True if the boundary is inclusive, false for exclusive
- `*typeCache`: Type cache entry containing type information for the boundary value
- `opfamily`: Operator family to use for finding the comparison operator
- `rng_collation`: Collation to use for the comparison operation
## Dependencies
- Functions called/Symbols referenced:
  - BTGreaterEqualStrategyNumber
  - BTGreaterStrategyNumber
  - BTLessEqualStrategyNumber
  - BTLessStrategyNumber
  - [get_opfamily_member](../g/get_opfamily_member.md)
  - [makeConst](../m/makeConst.md)
  - [make_opclause](../m/make_opclause.md)
  - OidIsValid
  - BOOLOID
  - InvalidOid
- Called from (representative examples):
  - [find_simplified_clause](../f/find_simplified_clause.md)

## Notes and Other Information
This function maps boundary conditions to B-tree strategy numbers: lower inclusive uses >=, lower exclusive uses >, upper inclusive uses <=, and upper exclusive uses <. It creates a properly typed constant expression from the boundary datum and constructs an OpExpr that can be evaluated by the query executor. The function returns NULL if it cannot find an appropriate operator in the specified operator family, which causes the calling optimization to fall back to the original range containment operation.

## Simplified Source

```c
static Expr *
build_bound_expr(Expr *elemExpr, Datum val,
				 bool isLowerBound, bool isInclusive,
				 TypeCacheEntry *typeCache,
				 Oid opfamily, Oid rng_collation)
{
	Oid			elemType = typeCache->type_id;
	int16		strategy;
	Oid			oproid;
	Expr	   *constExpr;

	// Select comparison operator based on boundary type
	if (isLowerBound)
		strategy = isInclusive ? BTGreaterEqualStrategyNumber : BTGreaterStrategyNumber;
	else
		strategy = isInclusive ? BTLessEqualStrategyNumber : BTLessStrategyNumber;

	// Find the appropriate operator in the operator family
	oproid = get_opfamily_member(opfamily, elemType, elemType, strategy);

	if (!OidIsValid(oproid))
		return NULL;

	// Create constant expression from boundary value
	constExpr = (Expr *) makeConst(elemType, -1, typeCache->typcollation,
								   typeCache->typlen, val, false, typeCache->typbyval);

	// Build and return comparison expression
	return make_opclause(oproid, BOOLOID, false, elemExpr, constExpr,
						 InvalidOid, rng_collation);
}
```