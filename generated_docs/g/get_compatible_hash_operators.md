# get_compatible_hash_operators

## Location
[src/backend/utils/cache/lsyscache.c:410-509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L410-L509)

## Overview
Retrieves hash equality operators compatible with a given operator, operating on its left-hand side and/or right-hand side datatypes.

## Definition

```c
bool
get_compatible_hash_operators(Oid opno,
							  Oid *lhs_opno, Oid *rhs_opno)
```
## Detailed Description
This function finds hash equality operators that are compatible with the input operator but operate on specific datatypes. It's particularly useful for cross-type operators where the left and right operand types differ, requiring separate single-type hash operators for each side.

The function searches pg_amop for hash operator family registrations of the input operator as an equality operator (HTEqualStrategyNumber). For cross-type operators, it then locates the corresponding single-type equality operators for the left and/or right operand types using get_opfamily_member.

If the input operator is already single-type (left and right types are the same), both output parameters receive the same operator OID. The function ensures atomic success/failure - if it cannot find operators for all requested sides, it resets the outputs and continues searching other operator families.

## Parameters / Member Variables
- `opno`: The OID of the input operator to find compatible hash operators for
- `*lhs_opno`: Optional output parameter for the left-hand side compatible operator (can be NULL)
- `*rhs_opno`: Optional output parameter for the right-hand side compatible operator (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheList1
  - ReleaseSysCacheList
  - [get_opfamily_member](get_opfamily_member.md)
  - HTEqualStrategyNumber
  - Form_pg_amop
  - [CatCList](../C/CatCList.md)
- Called from (representative examples):
  - [ExecInitSubPlan](../E/ExecInitSubPlan.md)
  - [create_unique_plan](../c/create_unique_plan.md)

## Notes and Other Information
- Returns true if able to find all requested operators, false otherwise
- Output parameters are initialized to InvalidOid on failure
- Only considers operators registered for the hash access method
- Handles both single-type and cross-type operators appropriately
- If multiple hash operator families contain the operator, uses the first valid match found
- Essential for hash join planning and subplan execution where hash compatibility is required

## Simplified Source

```c
bool
get_compatible_hash_operators(Oid opno, Oid *lhs_opno, Oid *rhs_opno)
{
    bool result = false;
    CatCList *catlist;

    // Initialize output parameters
    if (lhs_opno)
        *lhs_opno = InvalidOid;
    if (rhs_opno)
        *rhs_opno = InvalidOid;

    // Search for operator in hash operator families
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));

    for (int i = 0; i < catlist->n_members; i++)
    {
        HeapTuple tuple = &catlist->members[i]->tuple;
        Form_pg_amop aform = (Form_pg_amop) GETSTRUCT(tuple);

        // Check if this is a hash equality operator
        if (aform->amopmethod == HASH_AM_OID &&
            aform->amopstrategy == HTEqualStrategyNumber)
        {
            // Handle single-type operator case
            if (aform->amoplefttype == aform->amoprighttype)
            {
                if (lhs_opno)
                    *lhs_opno = opno;
                if (rhs_opno)
                    *rhs_opno = opno;
                result = true;
                break;
            }

            // Handle cross-type operator case
            // Find matching single-type operators for each side
            if (lhs_opno)
            {
                *lhs_opno = get_opfamily_member(aform->amopfamily,
                                                aform->amoplefttype,
                                                aform->amoplefttype,
                                                HTEqualStrategyNumber);
                if (!OidIsValid(*lhs_opno))
                    continue;
            }

            if (rhs_opno)
            {
                *rhs_opno = get_opfamily_member(aform->amopfamily,
                                                aform->amoprighttype,
                                                aform->amoprighttype,
                                                HTEqualStrategyNumber);
                if (!OidIsValid(*rhs_opno))
                {
                    // Reset LHS if RHS lookup failed
                    if (lhs_opno)
                        *lhs_opno = InvalidOid;
                    continue;
                }
            }

            result = true;
            break;
        }
    }

    ReleaseSysCacheList(catlist);
    return result;
}
```