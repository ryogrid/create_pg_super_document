# gin_extract_jsonb_query_path

## Location
src/backend/utils/adt/jsonb_gin.c: 1180 - 1219

## Overview
The query extraction function for the jsonb_path_ops GIN opclass that extracts search keys from query values for index lookups using path-sensitive hashing.

## Definition
```c
Datum gin_extract_jsonb_query_path(PG_FUNCTION_ARGS)
```

## Detailed Description
This function handles query processing for the jsonb_path_ops GIN opclass by extracting appropriate search keys from query values. For containment queries (@>), it delegates to gin_extract_jsonb_path to generate the same type of path-sensitive hashes used during indexing. For JSONPath queries, it uses extract_jsp_query to process the path expression and generate relevant search terms.

The function handles different query strategies appropriately: containment queries are processed using the same extraction logic as indexed values to ensure matching, while JSONPath queries require specialized processing to identify which indexed values might satisfy the path expression. Empty query results trigger a full index scan mode since no specific keys can guide the search.

## Parameters / Member Variables
- `query`: Input query value (JSONB for containment, JsonPath for JSONPath queries)
- `nentries`: Output parameter for the number of extracted search keys
- `strategy`: Strategy number indicating the query operation type
- `searchMode`: Output parameter indicating the search mode (normal or full scan)
- `extra_data`: Output parameter for additional query processing data

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER
  - PG_GETARG_UINT16
  - PG_GETARG_DATUM
  - PG_GETARG_JSONPATH_P
  - DirectFunctionCall2
  - gin_extract_jsonb_path
  - extract_jsp_query
  - DatumGetPointer
  - PointerGetDatum
  - elog
  - PG_RETURN_POINTER
- Strategy constants:
  - JsonbContainsStrategyNumber
  - JsonbJsonpathPredicateStrategyNumber
  - JsonbJsonpathExistsStrategyNumber
- Search modes:
  - GIN_SEARCH_MODE_ALL
- Types:
  - JsonPath
  - Datum
  - Pointer
- Called from: GIN index access method during query planning

## Notes and Other Information
- Works exclusively with jsonb_path_ops opclass, not standard jsonb_ops
- For containment queries, reuses the same extraction logic as indexing to ensure consistency
- Empty containment queries ("contains {}") require full index scan since no specific keys can guide the search
- JSONPath queries use specialized extraction logic to identify relevant indexed values
- Sets search mode to GIN_SEARCH_MODE_ALL when no specific keys can guide the search
- Supports only containment (@>) and JSONPath query strategies
- Located in src/backend/utils/adt/jsonb_gin.c:1180-1219