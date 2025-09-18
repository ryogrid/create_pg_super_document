# op_mergejoinable

## Location
src/backend/utils/cache/lsyscache.c: 1386 - 1436

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
  - lookup_type_cache
  - TYPECACHE_CMP_PROC
  - SearchSysCache1
  - HeapTupleIsValid
  - GETSTRUCT
  - ReleaseSysCache
  - Form_pg_operator
- Called from (representative examples):
  - compute_semijoin_info
  - check_mergejoinable

## Notes and Other Information
- The function provides special handling for ARRAY_EQ_OP and RECORD_EQ_OP operators
- For array equality, mergejoinability depends on whether the array elements can be compared using btarraycmp
- For record equality, it depends on whether records can be compared using btrecordcmp
- The oprcanmerge flag in pg_operator is just a hint; actual merge join plans require suitable btree opfamily entries
- This function is primarily used during query planning to optimize join operations