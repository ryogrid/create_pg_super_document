# get_op_hash_functions

## Location
[src/backend/utils/cache/lsyscache.c:510-600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L510-L600)

## Overview
Retrieves the OID(s) of standard hash support functions compatible with a given operator, operating on its left-hand side (LHS) and/or right-hand side (RHS) datatypes as required.

## Definition

```c
bool
get_op_hash_functions(Oid opno,
					  RegProcedure *lhs_procno, RegProcedure *rhs_procno)
```
## Detailed Description
This function searches for hash support functions associated with an operator by examining the pg_amop system catalog. It looks for entries where the operator is registered as the equality operator ("=") in hash operator families. The function can retrieve hash functions for both the left-hand side and right-hand side datatypes of the operator, which is particularly important for cross-type operators where the LHS and RHS types differ.

The function searches through all hash opfamilies that contain the given operator and attempts to find the appropriate HASHSTANDARD_PROC support functions. For single-type operators, the same hash function is used for both sides. For cross-type operators, different hash functions may be required for each datatype.

## Parameters / Member Variables
- : The OID of the operator for which to find hash support functions
- : Output parameter for the hash function OID for the left-hand side datatype (can be NULL if not needed)
- : Output parameter for the hash function OID for the right-hand side datatype (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheList1
  - [get_opfamily_proc](get_opfamily_proc.md)
  - ReleaseSysCacheList
  - HTEqualStrategyNumber
  - HASHSTANDARD_PROC
- Called from (representative examples):
  - execTuplesHashPrepare
  - ExecHashTableCreate
  - [ExecInitMemoize](../E/ExecInitMemoize.md)
  - [ExecInitSubPlan](../E/ExecInitSubPlan.md)
  - [convert_saop_to_hashed_saop_walker](../c/convert_saop_to_hashed_saop_walker.md)

## Notes and Other Information
- Returns true if able to find the requested function(s), false otherwise
- A false return indicates that the operator should not have been marked as oprcanhash
- Output parameters are initialized to InvalidOid on failure
- The function handles both single-type and cross-type operators appropriately
- If multiple opfamilies contain the operator, any compatible one can be used
- Located in src/backend/utils/cache/lsyscache.c at lines 510-600