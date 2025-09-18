# op_volatile

## Location
src/backend/utils/cache/lsyscache.c: 1493 - 1508

## Overview
Retrieves the volatility level of an operator by checking the provolatile flag of its underlying function.

## Definition
```c
char op_volatile(Oid opno)
```

## Detailed Description
This function determines the volatility level of an operator by examining the volatility property of its underlying function. Volatility indicates how the function behaves with respect to database changes and whether it can return different results when called with the same arguments. The function first obtains the OID of the implementing function using get_opcode(), then retrieves the volatility classification using func_volatile(). This information is crucial for query optimization decisions such as expression evaluation, indexing, and caching.

## Parameters / Member Variables
- `opno`: The OID of the operator whose volatility level is to be determined

## Dependencies
- Functions called/Symbols referenced:
  - [get_opcode](../g/get_opcode.md)
  - [func_volatile](../f/func_volatile.md)
  - elog
  - RegProcedure
- Called from (representative examples):
  - [lookup_proof_cache](../l/lookup_proof_cache.md)
  - [match_clause_to_partition_key](../m/match_clause_to_partition_key.md)

## Notes and Other Information
- Returns a character representing the volatility level: 'i' (immutable), 's' (stable), or 'v' (volatile)
- Immutable operators always return the same result for the same inputs
- Stable operators return consistent results within a single query but may change between queries
- Volatile operators can return different results even within the same query
- This information affects whether expressions can be pre-evaluated, cached, or used in indexes
- The function will raise an ERROR if the operator OID does not exist or has no associated function
- Used primarily in query optimization and partition pruning logic