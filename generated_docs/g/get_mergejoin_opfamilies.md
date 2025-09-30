# get_mergejoin_opfamilies

## Location
[src/backend/utils/cache/lsyscache.c:366-409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L366-L409)

## Overview
Returns a list of btree operator family OIDs in which a given operator represents equality for merge join operations.

## Definition

```c
List *
get_mergejoin_opfamilies(Oid opno)
```
## Detailed Description
This function searches for all btree operator families where the specified operator is registered as an equality operator (BTEqualStrategyNumber). The result is essential for merge join planning, as merge joins require equality operators that belong to compatible operator families.

An operator can potentially be registered as equality in multiple operator families, making the list return type necessary. The function searches pg_amop system catalog entries for the operator and collects all btree families where it serves as the equality operator.

The function is designed to support the planner's optimization decisions, particularly for recognizing when two expressions can be merged-joined. The list ordering is typically sorted by OID due to syscache implementation, which the planner relies on for consistent comparisons.

## Parameters / Member Variables
- : The OID of the operator to examine for merge join compatibility

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheList1
  - ReleaseSysCacheList
  - [lappend_oid](../l/lappend_oid.md)
  - Form_pg_amop
  - [CatCList](../C/CatCList.md)
- Called from (representative examples):
  - [match_eclasses_to_foreign_key_col](../m/match_eclasses_to_foreign_key_col.md)
  - [make_pathkey_from_sortinfo](../m/make_pathkey_from_sortinfo.md)
  - [compute_semijoin_info](../c/compute_semijoin_info.md)
  - [check_mergejoinable](../c/check_mergejoinable.md)

## Notes and Other Information
- Returns NIL if the operator is not found in any btree opfamilies as an equality operator
- The result list is typically ordered by OID, which the planner depends on for equality comparisons
- Only considers operators registered for the btree access method with BTEqualStrategyNumber strategy
- Critical for merge join planning and optimization opportunity recognition
- [List](../L/List.md) ordering may be unspecified when system index usage is disabled, potentially affecting planner optimization

## Simplified Source

```c
List *
get_mergejoin_opfamilies(Oid opno)
{
    List       *result = NIL;
    CatCList   *catlist;
    int         i;

    // Search pg_amop catalog for the operator
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));

    // Check each catalog entry
    for (i = 0; i < catlist->n_members; i++)
    {
        HeapTuple   tuple = &catlist->members[i]->tuple;
        Form_pg_amop aform = (Form_pg_amop) GETSTRUCT(tuple);

        // Add to result if this is a btree equality operator
        if (aform->amopmethod == BTREE_AM_OID &&
            aform->amopstrategy == BTEqualStrategyNumber)
            result = lappend_oid(result, aform->amopfamily);
    }

    ReleaseSysCacheList(catlist);
    return result;
}
```