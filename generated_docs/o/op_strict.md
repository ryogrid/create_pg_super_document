# op_strict

## Location
[src/backend/utils/cache/lsyscache.c:1477-1492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1477-L1492)

## Overview
Retrieves the strictness property of an operator by checking the proisstrict flag of its underlying function.

## Definition
```c
bool op_strict(Oid opno)
```

## Detailed Description
This function determines whether an operator is strict, meaning it returns NULL whenever any of its arguments is NULL. It works by first obtaining the OID of the function that implements the operator using get_opcode(), then checking the strictness of that function using func_strict(). Strict operators have important implications for query optimization, particularly in predicate evaluation and NULL handling.

## Parameters / Member Variables
- `opno`: The OID of the operator whose strictness is to be determined

## Dependencies
- Functions called/Symbols referenced:
  - [get_opcode](../g/get_opcode.md)
  - [func_strict](../f/func_strict.md)
  - elog
  - RegProcedure
- Called from (representative examples):
  - [ExecHashTableCreate](../E/ExecHashTableCreate.md)
  - [clause_is_strict_for](../c/clause_is_strict_for.md)
  - [operator_predicate_proof](operator_predicate_proof.md)
  - [have_partkey_equi_join](../h/have_partkey_equi_join.md)
  - [match_clause_to_partition_key](../m/match_clause_to_partition_key.md)

## Notes and Other Information
- A strict operator returns NULL if any input is NULL, without evaluating the function
- This property is crucial for query optimization, especially in predicate pushdown and NULL-aware optimizations
- The function will raise an ERROR if the operator OID does not exist or has no associated function
- Strictness information is used in partition pruning and join optimization
- This is a simple wrapper that delegates the actual strictness check to the function-level func_strict() function