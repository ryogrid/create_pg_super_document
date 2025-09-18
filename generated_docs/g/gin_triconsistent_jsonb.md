# gin_triconsistent_jsonb

## Location
[src/backend/utils/adt/jsonb_gin.c:1013-1089](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L1013-L1089)

## Overview
The ternary consistency check function for the JSONB GIN index that provides more precise matching logic by returning three-valued results (TRUE/MAYBE/FALSE) instead of just boolean values.

## Definition
```c
Datum gin_triconsistent_jsonb(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the GIN ternary consistency interface for JSONB data types, providing enhanced query optimization by distinguishing between definite matches, possible matches, and definite non-matches. Unlike the regular consistency function, it uses GinTernaryValue to represent three states: GIN_TRUE (definite match), GIN_MAYBE (possible match requiring recheck), and GIN_FALSE (definite non-match).

The function never returns GIN_TRUE directly, only GIN_MAYBE or GIN_FALSE, because the same structural limitations that affect the regular consistency function apply here - the GIN index cannot fully determine JSONB structural relationships. However, it can definitively rule out non-matches in certain cases, providing better query performance.

## Parameters / Member Variables
- `check`: Array of GinTernaryValue indicating the ternary state of each query key in the index
- `strategy`: Strategy number indicating the type of JSONB query operation
- `query`: The JSONB query value (commented out parameter, not actively used)  
- `nkeys`: Number of query keys to evaluate
- `extra_data`: Additional data for complex queries (used for JSONPath operations)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER
  - PG_GETARG_UINT16  
  - PG_GETARG_INT32
  - [execute_jsp_gin_node](../e/execute_jsp_gin_node.md)
  - PG_RETURN_GIN_TERNARY_VALUE
  - elog
- Types and constants:
  - GinTernaryValue
  - GIN_TRUE, GIN_MAYBE, GIN_FALSE
  - JsonbContainsStrategyNumber
  - JsonbExistsStrategyNumber
  - JsonbExistsAnyStrategyNumber
  - JsonbExistsAllStrategyNumber
  - JsonbJsonpathPredicateStrategyNumber
  - JsonbJsonpathExistsStrategyNumber
- Called from: GIN index access method during query execution

## Notes and Other Information
- Provides better performance than regular consistency function by definitively ruling out non-matches
- For containment and exists-all queries, returns GIN_FALSE if any required key is definitively absent
- For exists and exists-any queries, returns GIN_FALSE only if all keys are definitively absent
- JSONPath queries use execute_jsp_gin_node with ternary logic support
- Never returns GIN_TRUE directly - always forces recheck via GIN_MAYBE for potential matches
- Located in src/backend/utils/adt/jsonb_gin.c:1013-1089