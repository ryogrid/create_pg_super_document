# equality_ops_are_compatible

## Location
[src/backend/utils/cache/lsyscache.c:698-748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L698-L748)

## Overview
Determines whether two equality operators have compatible semantics by checking if they belong to the same btree or hash operator family.

## Definition
```c
bool equality_ops_are_compatible(Oid opno1, Oid opno2)
```

## Detailed Description
This function determines whether two equality operators can be considered semantically compatible. Compatibility is established through several checks:

1. **Identity check**: If both operators are the same (opno1 == opno2), they are trivially compatible.

2. **Opfamily membership**: The function searches through all pg_amop entries for the first operator (opno1) and checks if the second operator (opno2) belongs to any of the same btree or hash operator families.

The rationale is that operators within the same opfamily are designed to have compatible notions of equality, even if they operate on different but related types (cross-type operators). For example, int24eq (comparing int2 and int4) and int4eq (comparing int4 and int4) would be considered compatible because they both belong to the same integer btree opfamily and represent the same conceptual equality relationship.

## Parameters / Member Variables
- `opno1`: OID of the first equality operator
- `opno2`: OID of the second equality operator

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheList1
  - [op_in_opfamily](../o/op_in_opfamily.md)
  - ReleaseSysCacheList
  - Form_pg_amop
  - BTREE_AM_OID
  - HASH_AM_OID
- Called from (representative examples):
  - [query_is_distinct_for](../q/query_is_distinct_for.md)

## Notes and Other Information
- Returns true if the operators are compatible, false otherwise
- Only considers btree and hash access method operator families
- Cross-type operators (e.g., int24eq vs int4eq) can be compatible if they're in the same opfamily
- This compatibility check is important for query optimization, particularly in determining when operations can be considered equivalent
- Located in src/backend/utils/cache/lsyscache.c at lines 698-748