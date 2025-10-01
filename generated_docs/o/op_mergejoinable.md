# op_mergejoinable

## Location
[src/backend/utils/cache/lsyscache.c:1386-1436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1386-L1436)

## Overview
Determines whether an operator is potentially mergejoinable, which affects the planner's ability to use merge join algorithms for query execution.

## Definition
```c
bool op_mergejoinable(Oid opno, Oid inputtype)
```

## Detailed Description
This function checks if an operator can potentially be used in merge join operations. For most operators, it simply checks the oprcanmerge flag in pg_operator. However, for specific operators like array_eq and record_eq, mergejoinability depends on whether the element or field types are sortable, which is determined by consulting the type cache. The function serves as a hint to the query planner about whether it should attempt to find merge join plans for this operator.

## Parameters / Member Variables
- `opno`: The OID of the operator to check for mergejoinability
- `inputtype`: The data type of the left input operand, needed for type-dependent mergejoinability checks

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - TYPECACHE_CMP_PROC
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_operator
- Called from (representative examples):
  - [compute_semijoin_info](../c/compute_semijoin_info.md)
  - [check_mergejoinable](../c/check_mergejoinable.md)

## Notes and Other Information
- The function provides special handling for ARRAY_EQ_OP and RECORD_EQ_OP operators
- For array equality, mergejoinability depends on whether the array elements can be compared using btarraycmp
- For record equality, it depends on whether records can be compared using btrecordcmp
- The oprcanmerge flag in pg_operator is just a hint; actual merge join plans require suitable btree opfamily entries
- This function is primarily used during query planning to optimize join operations

## Simplified Source

```c
bool op_mergejoinable(Oid opno, Oid inputtype) {
    bool result = false;
    HeapTuple tp;
    TypeCacheEntry *typentry;

    // Special handling for array equality - check if elements are sortable
    if (opno == ARRAY_EQ_OP) {
        typentry = lookup_type_cache(inputtype, TYPECACHE_CMP_PROC);
        if (typentry->cmp_proc == F_BTARRAYCMP)
            result = true;
    }
    // Special handling for record equality - check if fields are sortable
    else if (opno == RECORD_EQ_OP) {
        typentry = lookup_type_cache(inputtype, TYPECACHE_CMP_PROC);
        if (typentry->cmp_proc == F_BTRECORDCMP)
            result = true;
    }
    // For other operators, check the oprcanmerge flag in pg_operator
    else {
        tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
        if (HeapTupleIsValid(tp)) {
            Form_pg_operator optup = (Form_pg_operator) GETSTRUCT(tp);
            result = optup->oprcanmerge;
            ReleaseSysCache(tp);
        }
    }

    return result;
}
```