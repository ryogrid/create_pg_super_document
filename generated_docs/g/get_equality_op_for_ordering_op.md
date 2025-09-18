# get_equality_op_for_ordering_op

## Location
src/backend/utils/cache/lsyscache.c: 267 - 304

## Overview
Retrieves the OID of the datatype-specific btree equality operator associated with a given ordering operator.

## Definition


## Detailed Description
This function takes an ordering operator ("<" or ">") and finds its corresponding equality operator ("=") within the same btree operator family. It leverages get_ordering_op_properties to first identify the operator family and strategy, then uses get_opfamily_member to locate the equality operator with BTEqualStrategyNumber.

The function also optionally reports whether the input operator represents a "reverse" ordering (greater-than) versus normal ordering (less-than), which is useful for understanding sort direction semantics.

## Parameters / Member Variables
- : The OID of the ordering operator ("<" or ">")
- : Optional output parameter; set to false for "<" operators, true for ">" operators (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [get_ordering_op_properties](get_ordering_op_properties.md)
  - [get_opfamily_member](get_opfamily_member.md)
  - BTGreaterStrategyNumber
- Called from (representative examples):
  - [show_sortorder_options](../s/show_sortorder_options.md)
  - preparePresortedCols
  - [create_unique_plan](../c/create_unique_plan.md)
  - [preprocess_minmax_aggregates](../p/preprocess_minmax_aggregates.md)
  - [addTargetToSortList](../a/addTargetToSortList.md)

## Notes and Other Information
- Returns InvalidOid if no matching equality operator can be found
- The reverse parameter is optional and can be passed as NULL if direction information is not needed
- Relies on btree operator family structure where equality, less-than, and greater-than operators are grouped together
- Essential for query planning operations that need to convert between ordering and equality semantics