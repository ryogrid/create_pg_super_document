# op_strict

## Location
src/backend/utils/cache/lsyscache.c: 1477 - 1492

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
  - get_opcode
  - func_strict
  - elog
  - RegProcedure
- Called from (representative examples):
  - ExecHashTableCreate
  - clause_is_strict_for
  - operator_predicate_proof
  - have_partkey_equi_join
  - match_clause_to_partition_key

## Notes and Other Information
- A strict operator returns NULL if any input is NULL, without evaluating the function
- This property is crucial for query optimization, especially in predicate pushdown and NULL-aware optimizations
- The function will raise an ERROR if the operator OID does not exist or has no associated function
- Strictness information is used in partition pruning and join optimization
- This is a simple wrapper that delegates the actual strictness check to the function-level func_strict() function