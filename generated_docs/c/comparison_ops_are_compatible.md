# comparison_ops_are_compatible

## Location
[src/backend/utils/cache/lsyscache.c:749-795](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L749-L795)

## Overview
Determines whether two comparison operators have compatible semantics by checking if they belong to the same btree operator family.

## Definition
```c
bool comparison_ops_are_compatible(Oid opno1, Oid opno2)
```

## Detailed Description
This function determines whether two comparison operators can be considered semantically compatible for ordering purposes. It is nearly identical to equality_ops_are_compatible() but focuses specifically on btree operator families since comparison operations require ordering semantics that hash opfamilies do not provide.

The function performs these compatibility checks:

1. **Identity check**: If both operators are the same (opno1 == opno2), they are trivially compatible.

2. **Btree opfamily membership**: The function searches through all pg_amop entries for the first operator (opno1) and checks if the second operator (opno2) belongs to any of the same btree operator families.

Operators within the same btree opfamily have compatible comparison semantics, meaning they impose the same ordering on values. For example, '<' and '>=' operators for integers would be considered compatible because they both belong to the integer btree opfamily and provide consistent ordering relationships.

## Parameters / Member Variables
- `opno1`: OID of the first comparison operator
- `opno2`: OID of the second comparison operator

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheList1
  - [op_in_opfamily](../o/op_in_opfamily.md)
  - ReleaseSysCacheList
  - Form_pg_amop
  - BTREE_AM_OID
- Called from (representative examples):
  - [ineq_histogram_selectivity](../i/ineq_histogram_selectivity.md)

## Notes and Other Information
- Returns true if the operators are compatible, false otherwise
- Only considers btree access method operator families (unlike equality_ops_are_compatible which also checks hash families)
- This is used in query optimization, particularly for selectivity estimation and plan generation
- Cross-type comparison operators can be compatible if they're in the same btree opfamily
- The function is essential for determining when different comparison operators can be substituted or combined in query plans
- Located in src/backend/utils/cache/lsyscache.c at lines 749-795

## Simplified Source
```c
bool comparison_ops_are_compatible(Oid opno1, Oid opno2) {
    // Same operator is always compatible
    if (opno1 == opno2)
        return true;

    // Search through all pg_amop entries for opno1
    CatCList *catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno1));

    bool result = false;
    for (int i = 0; i < catlist->n_members; i++) {
        HeapTuple op_tuple = &catlist->members[i]->tuple;
        Form_pg_amop op_form = (Form_pg_amop) GETSTRUCT(op_tuple);

        // Check if both operators belong to the same btree family
        if (op_form->amopmethod == BTREE_AM_OID) {
            if (op_in_opfamily(opno2, op_form->amopfamily)) {
                result = true;
                break;
            }
        }
    }

    ReleaseSysCacheList(catlist);
    return result;
}
```