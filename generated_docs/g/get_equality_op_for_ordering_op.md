# get_equality_op_for_ordering_op

## Location
[src/backend/utils/cache/lsyscache.c:267-304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L267-L304)

## Overview
Retrieves the OID of the datatype-specific btree equality operator associated with a given ordering operator.

## Definition

```c
Oid
get_equality_op_for_ordering_op(Oid opno, bool *reverse)
```
## Detailed Description
This function takes an ordering operator ("<" or ">") and finds its corresponding equality operator ("=") within the same btree operator family. It leverages get_ordering_op_properties to first identify the operator family and strategy, then uses get_opfamily_member to locate the equality operator with BTEqualStrategyNumber.

The function also optionally reports whether the input operator represents a "reverse" ordering (greater-than) versus normal ordering (less-than), which is useful for understanding sort direction semantics.

## Parameters / Member Variables
- `opno`: The OID of the ordering operator ("<" or ">")
- `*reverse`: Optional output parameter; set to false for "<" operators, true for ">" operators (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [get_ordering_op_properties](get_ordering_op_properties.md)
  - [get_opfamily_member](get_opfamily_member.md)
  - BTGreaterStrategyNumber
- Called from (representative examples):
  - [show_sortorder_options](../s/show_sortorder_options.md)
  - [preparePresortedCols](../p/preparePresortedCols.md)
  - [create_unique_plan](../c/create_unique_plan.md)
  - [preprocess_minmax_aggregates](../p/preprocess_minmax_aggregates.md)
  - [addTargetToSortList](../a/addTargetToSortList.md)

## Notes and Other Information
- Returns InvalidOid if no matching equality operator can be found
- The reverse parameter is optional and can be passed as NULL if direction information is not needed
- Relies on btree operator family structure where equality, less-than, and greater-than operators are grouped together
- Essential for query planning operations that need to convert between ordering and equality semantics

## Simplified Source

```c
Oid get_equality_op_for_ordering_op(Oid opno, bool *reverse)
{
    Oid result = InvalidOid;
    Oid opfamily;
    Oid opcintype;
    int16 strategy;

    // Find the operator properties in the operator family system
    if (get_ordering_op_properties(opno, &opfamily, &opcintype, &strategy))
    {
        // Get the equality operator from the same operator family
        result = get_opfamily_member(opfamily,
                                     opcintype,
                                     opcintype,
                                     BTEqualStrategyNumber);

        // Set reverse flag: true for ">" operators, false for "<" operators
        if (reverse)
            *reverse = (strategy == BTGreaterStrategyNumber);
    }

    return result;  // InvalidOid if no equality operator found
}
```