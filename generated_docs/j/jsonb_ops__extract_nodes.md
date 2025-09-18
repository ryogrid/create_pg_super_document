# jsonb_ops__extract_nodes

## Location
src/backend/utils/adt/jsonb_gin.c: 408 - 477

## Overview
Extracts and constructs a list of GIN index nodes from a JSON path for the jsonb_ops operator class, handling both path entries and scalar values with appropriate indexing logic.

## Definition
```c
static List *
jsonb_ops__extract_nodes(JsonPathGinContext *cxt, JsonPathGinPath path,
                         JsonbValue *scalar, List *nodes)
```

## Detailed Description
This function is responsible for extracting indexable nodes from JSON path expressions specifically for the jsonb_ops GIN operator class. It processes both the path structure and scalar values to generate appropriate index entries. The function handles different scenarios including lax/strict mode behavior, string interpretation as keys vs values, and creates complex OR expressions when ambiguity exists. For string scalars in particular, it determines whether they should be treated as keys or non-key entries based on the path context and mode, potentially creating OR nodes to cover both possibilities.

## Parameters / Member Variables
- `cxt`: JsonPathGinContext containing extraction context including lax/strict mode information
- `path`: JsonPathGinPath representing the path structure to be processed
- `scalar`: JsonbValue pointer to the scalar value being indexed (can be NULL)
- `nodes`: List of existing nodes to append to

## Dependencies
- Functions called/Symbols referenced:
  - make_jsp_entry_node
  - make_jsp_entry_node_scalar
  - make_jsp_expr_node_binary
  - lappend
  - JsonPathGinPathItem
  - JsonPathGinNode
  - GinTernaryValue
  - Various enum values (jpiKey, jbvString, JSP_GIN_OR, GIN_TRUE, GIN_FALSE, GIN_MAYBE)
- Called from (representative examples):
  - extract_jsp_query

## Notes and Other Information
- This is a static function within the JSONB GIN indexing module
- Handles the jsonb_ops operator class which interprets array elements as potential keys
- Creates OR expressions when string scalars could be interpreted as both keys and non-keys
- The lax mode affects how strings in array contexts are interpreted
- Part of PostgreSQL's GIN indexing infrastructure for efficient JSONB path queries
- Located in src/backend/utils/adt/jsonb_gin.c:408-477