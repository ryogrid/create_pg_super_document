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

## Simplified Source

```c
Datum
gin_triconsistent_jsonb(PG_FUNCTION_ARGS)
{
    GinTernaryValue *check = (GinTernaryValue *) PG_GETARG_POINTER(0);
    StrategyNumber strategy = PG_GETARG_UINT16(1);
    int32 nkeys = PG_GETARG_INT32(3);
    Pointer *extra_data = (Pointer *) PG_GETARG_POINTER(4);
    GinTernaryValue res = GIN_MAYBE;
    int32 i;

    // Never return GIN_TRUE - always force recheck due to structural limitations

    if (strategy == JsonbContainsStrategyNumber ||
        strategy == JsonbExistsAllStrategyNumber)
    {
        // For @> and ?& operators: all keys must be present
        for (i = 0; i < nkeys; i++)
        {
            if (check[i] == GIN_FALSE)
            {
                res = GIN_FALSE;  // Definite non-match if any key absent
                break;
            }
        }
        // Otherwise remain GIN_MAYBE
    }
    else if (strategy == JsonbExistsStrategyNumber ||
             strategy == JsonbExistsAnyStrategyNumber)
    {
        // For ? and ?| operators: at least one key must be present
        res = GIN_FALSE;  // Start pessimistic
        for (i = 0; i < nkeys; i++)
        {
            if (check[i] == GIN_TRUE || check[i] == GIN_MAYBE)
            {
                res = GIN_MAYBE;  // Found potential match
                break;
            }
        }
        // If no keys are possible, remains GIN_FALSE
    }
    else if (strategy == JsonbJsonpathPredicateStrategyNumber ||
             strategy == JsonbJsonpathExistsStrategyNumber)
    {
        // For JSONPath queries: evaluate expression tree with ternary logic
        if (nkeys > 0)
        {
            Assert(extra_data && extra_data[0]);
            res = execute_jsp_gin_node((JsonPathGinNode *) extra_data[0],
                                      check, true);

            // Force recheck even for GIN_TRUE results
            if (res == GIN_TRUE)
                res = GIN_MAYBE;
        }
    }
    else
        elog(ERROR, "unrecognized strategy number: %d", strategy);

    PG_RETURN_GIN_TERNARY_VALUE(res);
}
```