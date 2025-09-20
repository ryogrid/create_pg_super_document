# make_pathkey_from_sortop

## Location
[src/backend/optimizer/path/pathkeys.c:255-301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L255-L301)

## Overview
Creates a canonical PathKey from a sort operator, serving as a compatibility wrapper that extracts operator properties and delegates to make_pathkey_from_sortinfo.

## Definition

```c
static PathKey *
make_pathkey_from_sortop(PlannerInfo *root,
						 Expr *expr,
						 Oid ordering_op,
						 bool nulls_first,
						 Index sortref,
						 bool create_it)
```
## Detailed Description
This function provides a simplified interface for creating PathKeys when only a sort operator OID is available, rather than detailed operator family information. It acts as a compatibility layer and convenience wrapper around make_pathkey_from_sortinfo.

The function performs the following key operations:

1. **Operator Property Extraction**: Uses get_ordering_op_properties to extract the operator family, input type, and strategy from the ordering operator OID.

2. **Collation Resolution**: Determines the appropriate collation by examining the expression, since SortGroupClause structures don't carry collation information.

3. **Strategy Conversion**: Converts the extracted strategy number to a boolean reverse_sort flag for make_pathkey_from_sortinfo.

4. **Delegation**: Calls make_pathkey_from_sortinfo with the extracted parameters, passing NULL for the rel parameter since no specific relation context is assumed.

The comment indicates this function is intended to eventually be phased out once SortGroupClause is restructured to provide more detailed sorting information directly.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context
- : The expression to be sorted on
- : OID of the ordering/comparison operator
- : Boolean indicating NULL value positioning
- : SortGroupRef from SortGroupClause, or zero if not applicable
- : Boolean controlling EquivalenceClass creation

## Dependencies
- Functions called/Symbols referenced:
  - [get_ordering_op_properties](../g/get_ordering_op_properties.md) (operator property extraction)
  - elog (error logging)
  - [exprCollation](../e/exprCollation.md) (collation extraction from expressions)
  - [make_pathkey_from_sortinfo](make_pathkey_from_sortinfo.md) (core PathKey creation)
  - BTGreaterStrategyNumber (strategy constant for comparison)
- Called from (representative examples):
  - [make_pathkeys_for_sortclauses_extended](make_pathkeys_for_sortclauses_extended.md)

## Notes and Other Information
- Intended as a temporary compatibility function pending SortGroupClause restructuring
- Automatically determines collation from the expression since SortGroupClause doesn't provide it
- Passes NULL for relation context, making it suitable for general-purpose PathKey creation
- Performs error checking to ensure the provided operator is a valid ordering operator
- Maps B-tree strategy numbers to boolean reverse_sort flags for interface compatibility
- Located in src/backend/optimizer/path/pathkeys.c:255-301