# jsonb_path_ops__extract_nodes

## Location
src/backend/utils/adt/jsonb_gin.c: 478 - 503

## Overview
Extracts GIN index nodes from a JSON path for the jsonb_path_ops operator class, creating hash-based index entries for equality queries only.

## Definition
```c
static List *
jsonb_path_ops__extract_nodes(JsonPathGinContext *cxt, JsonPathGinPath path,
                              JsonbValue *scalar, List *nodes)
```

## Detailed Description
This function handles node extraction for the jsonb_path_ops GIN operator class, which uses a different indexing strategy compared to jsonb_ops. It creates hash-based index entries by combining the path hash with the scalar value hash. Unlike jsonb_ops, this operator class only supports equality queries and does not index path existence queries, making it more compact but less flexible. When no scalar value is provided, the function returns the nodes unchanged since jsonb_path_ops doesn't support EXISTS queries.

## Parameters / Member Variables
- `cxt`: JsonPathGinContext containing extraction context (unused in this function)
- `path`: JsonPathGinPath containing the path structure and its precomputed hash
- `scalar`: JsonbValue pointer to the scalar value being indexed (can be NULL)
- `nodes`: List of existing nodes to append to

## Dependencies
- Functions called/Symbols referenced:
  - JsonbHashScalarValue
  - make_jsp_entry_node
  - UInt32GetDatum
  - lappend
  - JsonPathGinContext
  - JsonPathGinPath
- Called from (representative examples):
  - extract_jsp_query

## Notes and Other Information
- This is a static function within the JSONB GIN indexing module
- Specific to the jsonb_path_ops operator class which prioritizes space efficiency over query flexibility
- Only supports equality queries, not EXISTS queries
- Uses hash-based indexing by combining path and scalar value hashes
- More compact than jsonb_ops but cannot answer some types of queries
- Part of PostgreSQL's GIN indexing infrastructure for efficient JSONB path queries
- Located in src/backend/utils/adt/jsonb_gin.c:478-503