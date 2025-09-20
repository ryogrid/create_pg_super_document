# extract_jsp_query

## Location
[src/backend/utils/adt/jsonb_gin.c:748-798](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L748-L798)

## Overview
Serves as the main entry point for extracting GIN index entries from jsonpath queries, coordinating the entire process from jsonpath parsing to entry collection and returning the final query data for index operations.

## Definition

```c
static Datum *
extract_jsp_query(JsonPath *jp, StrategyNumber strat, bool pathOps,
				  int32 *nentries, Pointer **extra_data)
```
## Detailed Description
This function orchestrates the complete jsonpath-to-GIN query extraction process. It first initializes the extraction context based on the specified operator class (jsonb_ops vs jsonb_path_ops), then parses the jsonpath and determines the appropriate extraction strategy based on the query type.

For EXISTS queries (JsonbJsonpathExistsStrategyNumber), it uses  to handle path existence checks. For other operations, it uses  to process boolean expressions. After building the query tree, it calls  to collect all GIN entries and prepare the final query representation.

The function configures different extraction behaviors for the two supported operator classes: jsonb_ops (which extracts keys and values separately) and jsonb_path_ops (which extracts hash-based path representations). It also respects the jsonpath LAX/STRICT mode setting.

## Parameters / Member Variables
- : JsonPath structure containing the parsed jsonpath expression
- : StrategyNumber indicating the type of jsonpath operation (EXISTS, boolean expressions, etc.)
- : Boolean flag indicating whether to use jsonb_path_ops (true) or jsonb_ops (false) extraction strategy
- : Output parameter receiving the number of GIN entries extracted
- : Output parameter receiving additional query data, with the root expression node at index 0

## Dependencies
- Functions called/Symbols referenced:
  - [jspInit](../j/jspInit.md) (initializes jsonpath item from JsonPath structure)
  - [extract_jsp_path_expr](extract_jsp_path_expr.md) (extracts path existence expressions)
  - [extract_jsp_bool_expr](extract_jsp_bool_expr.md) (extracts boolean expressions)
  - [emit_jsp_gin_entries](emit_jsp_gin_entries.md) (collects GIN entries from the query tree)
  - [jsonb_ops__add_path_item](../j/jsonb_ops__add_path_item.md), jsonb_path_ops__add_path_item (operator class-specific path handlers)
  - [jsonb_ops__extract_nodes](../j/jsonb_ops__extract_nodes.md), jsonb_path_ops__extract_nodes (operator class-specific node extractors)
  - [palloc0](../p/palloc0.md) (memory allocation)
- Called from (representative examples):
  - [gin_extract_jsonb_query](../g/gin_extract_jsonb_query.md) (main GIN extraction entry point for jsonb_ops)
  - [gin_extract_jsonb_query_path](../g/gin_extract_jsonb_query_path.md) (main GIN extraction entry point for jsonb_path_ops)

## Notes and Other Information
- Returns NULL if no extractable entries are found, indicating the query cannot be optimized using GIN indexes
- The function automatically detects LAX vs STRICT jsonpath mode from the JsonPath header
- The extra_data array's first element always contains the root expression node for query execution
- Different operator classes use different extraction strategies optimized for their respective indexing approaches
- Memory allocation for extra_data is sized based on the number of entries but only the first element is currently used