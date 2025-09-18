# op_hashjoinable

## Location
[src/backend/utils/cache/lsyscache.c:1437-1476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1437-L1476)

## Overview
Determines whether an operator can be used in hash join operations by checking if it has appropriate hash functionality.

## Definition
```c
bool op_hashjoinable(Oid opno, Oid inputtype)
```

## Detailed Description
This function evaluates if an operator is suitable for hash join algorithms. For most operators, it checks the oprcanhash flag in the pg_operator system catalog. However, for special cases like array_eq and record_eq operators, hashjoinability depends on whether the element or field types have appropriate hash functions available. The function consults the type cache to determine if the required hash procedures (F_HASH_ARRAY or F_HASH_RECORD) are available for the specific input data type.

## Parameters / Member Variables
- `opno`: The OID of the operator to check for hashjoinability
- `inputtype`: The data type of the left input operand, needed for type-dependent hashjoinability checks

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - TYPECACHE_HASH_PROC
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_operator
- Called from (representative examples):
  - [generate_join_implied_equalities_normal](../g/generate_join_implied_equalities_normal.md)
  - [compute_semijoin_info](../c/compute_semijoin_info.md)
  - [check_hashjoinable](../c/check_hashjoinable.md)
  - [hash_ok_operator](../h/hash_ok_operator.md)
  - [addTargetToSortList](../a/addTargetToSortList.md)

## Notes and Other Information
- Similar to op_mergejoinable but focuses on hash join capability rather than merge join capability
- Provides special handling for ARRAY_EQ_OP and RECORD_EQ_OP operators
- For array equality, requires F_HASH_ARRAY hash procedure to be available
- For record equality, requires F_HASH_RECORD hash procedure to be available
- The oprcanhash flag in pg_operator must be true and suitable hash opfamily entries must exist
- Critical for query planner decisions about using hash join algorithms