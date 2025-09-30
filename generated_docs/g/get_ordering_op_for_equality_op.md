# get_ordering_op_for_equality_op

## Location
[src/backend/utils/cache/lsyscache.c:305-365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L305-L365)

## Overview
Retrieves the OID of a datatype-specific btree ordering operator associated with a given equality operator.

## Definition

```c
Oid
get_ordering_op_for_equality_op(Oid opno, bool use_lhs_type)
```
## Detailed Description
This function performs the inverse operation of get_equality_op_for_ordering_op. Given an equality operator, it finds a compatible ordering operator (specifically a "<" operator) within the same btree operator family. This is primarily used when the system needs to sort data before performing unique-ification operations.

The function searches pg_amop for registrations of the equality operator in btree operator families, then locates the corresponding less-than operator. Since the equality operator might be cross-type, the caller can specify whether to use the left-hand or right-hand side data type for finding the single-data-type ordering operator.

If multiple possibilities exist, the function returns the first valid one found, as any compatible ordering operator will suffice for sorting purposes.

## Parameters / Member Variables
- : The OID of the equality operator
- : Boolean flag indicating whether to use the left-hand side data type (true) or right-hand side data type (false) when searching for the ordering operator

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheList1
  - ReleaseSysCacheList
  - [get_opfamily_member](get_opfamily_member.md)
  - BTLessStrategyNumber
  - Form_pg_amop
  - [CatCList](../C/CatCList.md)
- Called from (representative examples):
  - [create_unique_plan](../c/create_unique_plan.md)

## Notes and Other Information
- Returns InvalidOid if no matching ordering operator can be found
- Primarily used in query planning for operations that require sorting before uniquification
- Handles cross-type equality operators by allowing selection of either left or right operand type
- Returns the first valid match found, as exact choice doesn't matter for sorting purposes
- Specifically looks for BTLessStrategyNumber operators ("<" operators) rather than greater-than operators

## Simplified Source

```c
Oid
get_ordering_op_for_equality_op(Oid opno, bool use_lhs_type)
{
    Oid result = InvalidOid;
    CatCList *catlist;

    // Search for equality operator in btree operator families
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));

    for (int i = 0; i < catlist->n_members; i++)
    {
        HeapTuple tuple = &catlist->members[i]->tuple;
        Form_pg_amop aform = (Form_pg_amop) GETSTRUCT(tuple);

        // Only consider btree operators
        if (aform->amopmethod != BTREE_AM_OID)
            continue;

        // Check if this is an equality operator
        if (aform->amopstrategy == BTEqualStrategyNumber)
        {
            // Get the appropriate data type (LHS or RHS)
            Oid typid = use_lhs_type ? aform->amoplefttype : aform->amoprighttype;

            // Find the corresponding less-than operator in the same family
            result = get_opfamily_member(aform->amopfamily,
                                         typid, typid,
                                         BTLessStrategyNumber);
            if (OidIsValid(result))
                break;
        }
    }

    ReleaseSysCacheList(catlist);
    return result;
}
```