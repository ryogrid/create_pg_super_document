# extract_jsp_path_expr

## Location
[src/backend/utils/adt/jsonb_gin.c:564-582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L564-L582)

## Overview
Extracts a single expression node from JSON path expressions, handling both EXISTS queries and equality comparisons by combining multiple extracted nodes with AND logic when necessary.

## Definition
```c
static JsonPathGinNode *
extract_jsp_path_expr(JsonPathGinContext *cxt, JsonPathGinPath path,
                      JsonPathItem *jsp, JsonbValue *scalar)
```

## Detailed Description
This function serves as a wrapper around extract_jsp_path_expr_nodes to create a single expression node from a JSON path. It handles two types of queries: EXISTS(jsp) when scalar is NULL, and jsp == scalar when scalar is provided. The function first extracts a list of nodes that need to be AND-ed together. If no nodes are extracted, it returns NULL indicating a full scan is needed. For a single node, it returns that node directly to avoid unnecessary overhead. For multiple nodes, it creates an AND expression node to combine them logically.

## Parameters / Member Variables
- `cxt`: JsonPathGinContext containing the extraction context and operator class-specific functions
- `path`: JsonPathGinPath representing the current path context
- `jsp`: JsonPathItem pointer to the JSON path item to process  
- `scalar`: JsonbValue pointer to the scalar value for equality queries (NULL for EXISTS queries)

## Dependencies
- Functions called/Symbols referenced:
  - [extract_jsp_path_expr_nodes](extract_jsp_path_expr_nodes.md)
  - [list_length](../l/list_length.md)
  - linitial
  - [make_jsp_expr_node_args](../m/make_jsp_expr_node_args.md)
  - [JsonPathGinContext](../J/JsonPathGinContext.md)
  - JsonPathGinPath
  - JsonPathItem
  - [JsonPathGinNode](../J/JsonPathGinNode.md)
  - JSP_GIN_AND
- Called from (representative examples):
  - [extract_jsp_bool_expr](extract_jsp_bool_expr.md)
  - [extract_jsp_query](extract_jsp_query.md)

## Notes and Other Information
- This is a static function within the JSONB GIN indexing module
- Returns NULL when no indexable conditions can be extracted, requiring a full scan
- Optimizes single-node cases by avoiding unnecessary AND expressions
- Combines multiple extracted nodes using AND logic for complex path expressions
- Part of PostgreSQL's GIN indexing infrastructure for efficient JSONB path queries
- Located in src/backend/utils/adt/jsonb_gin.c:564-582

## Simplified Source

```c
static JsonPathGinNode *
extract_jsp_path_expr(JsonPathGinContext *cxt, JsonPathGinPath path,
                      JsonPathItem *jsp, JsonbValue *scalar)
{
    // Extract nodes that need to be AND-ed together
    List *nodes = extract_jsp_path_expr_nodes(cxt, path, jsp, scalar);

    if (nodes == NIL)
        // No extractable nodes - full scan needed
        return NULL;

    if (list_length(nodes) == 1)
        // Single node - return directly
        return linitial(nodes);

    // Multiple nodes - create AND expression
    return make_jsp_expr_node_args(JSP_GIN_AND, nodes);
}
```