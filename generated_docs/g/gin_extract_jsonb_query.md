# gin_extract_jsonb_query

## Location
[src/backend/utils/adt/jsonb_gin.c:848-928](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L848-L928)

## Overview
Main GIN query extraction function for the jsonb_ops operator class, handling multiple jsonb query strategies including containment, key existence, and jsonpath operations by dispatching to appropriate extraction methods.

## Definition

```c
struct_array_builtin(query, TEXTOID, &key_datums, &key_nulls, &key_count);
```
## Detailed Description
This function serves as the primary entry point for GIN query extraction in the jsonb_ops operator class. It implements a strategy-based dispatch system that handles five different types of jsonb queries:

1. **JsonbContainsStrategyNumber**: For  containment queries, delegates to the standard  function
2. **JsonbExistsStrategyNumber**: For  key existence queries, creates a single GIN entry for the specified text key
3. **JsonbExistsAnyStrategyNumber/JsonbExistsAllStrategyNumber**: For  and  multi-key queries, creates GIN entries for each non-null key in the text array
4. **JsonbJsonpathPredicateStrategyNumber/JsonbJsonpathExistsStrategyNumber**: For  and  jsonpath queries, uses  for advanced path-based extraction

The function also sets appropriate search modes: when no entries can be extracted (indicating the query cannot be optimized), it sets GIN_SEARCH_MODE_ALL to force a full index scan. Special cases include empty jsonb containment queries () and JsonbExistsAll queries with no keys, both requiring full scans.

## Parameters / Member Variables
This function uses PostgreSQL's function call convention with PG_FUNCTION_ARGS:
- : Various query arguments depending on strategy (jsonb, text, text array, or jsonpath)
- : Output parameter for the number of extracted GIN entries
- : StrategyNumber indicating the type of query operation
- : Output parameter controlling GIN search behavior
- : Output parameter for additional query data (used by jsonpath operations)

## Dependencies
- Functions called/Symbols referenced:
  - [gin_extract_jsonb](gin_extract_jsonb.md) (standard jsonb GIN extraction for containment queries)
  - [make_text_key](../m/make_text_key.md) (creates GIN entries for text keys with JGINFLAG_KEY flag)
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md) (extracts elements from text arrays)
  - [extract_jsp_query](../e/extract_jsp_query.md) (handles jsonpath query extraction)
  - DirectFunctionCall2 (PostgreSQL function call mechanism)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
- Called from (representative examples):
  - This is a top-level PostgreSQL function called by the GIN access method during query planning

## Notes and Other Information
- The function is registered as a PostgreSQL C function and called directly by the GIN access method
- Uses PostgreSQL's function call convention (PG_FUNCTION_ARGS/PG_RETURN_POINTER)
- Handles memory allocation for the returned entries array using palloc
- Sets GIN_SEARCH_MODE_ALL for queries that cannot be optimized, ensuring correctness at the cost of performance
- Filters out NULL values from text arrays in exists-any/exists-all operations
- The extra_data parameter is only populated for jsonpath operations and contains the query execution tree