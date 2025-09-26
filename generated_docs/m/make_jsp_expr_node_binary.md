# make_jsp_expr_node_binary

## Location
[src/backend/utils/adt/jsonb_gin.c:395-407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L395-L407)

## Overview
Creates a binary JSON path GIN expression node with two child arguments for use in GIN index operations on JSONB data.

## Definition

```c
static JsonPathGinNode *
make_jsp_expr_node_binary(JsonPathGinNodeType type,
						  JsonPathGinNode *arg1, JsonPathGinNode *arg2)
```
## Detailed Description
This function is a helper utility that constructs a binary expression node in the JSON path GIN index tree structure. It creates a new JsonPathGinNode with exactly two child arguments, which is commonly needed for binary operations like logical AND/OR operations or comparison operations in JSON path expressions. The function serves as a convenience wrapper around  specifically for binary operations.

## Parameters / Member Variables
- : The JsonPathGinNodeType that specifies what kind of binary operation this node represents
- : The first child argument node in the binary expression
- : The second child argument node in the binary expression

## Dependencies
- Functions called/Symbols referenced:
  - [make_jsp_expr_node](make_jsp_expr_node.md)
  - [JsonPathGinNodeType](../J/JsonPathGinNodeType.md)
  - [JsonPathGinNode](../J/JsonPathGinNode.md)
- Called from (representative examples):
  - [jsonb_ops__extract_nodes](../j/jsonb_ops__extract_nodes.md)
  - [extract_jsp_bool_expr](../e/extract_jsp_bool_expr.md)

## Notes and Other Information
- This is a static function within the JSONB GIN indexing module
- Part of the PostgreSQL GIN (Generalized Inverted Index) infrastructure for efficient JSONB querying
- The function assumes exactly 2 arguments and allocates space accordingly
- Located in src/backend/utils/adt/jsonb_gin.c:395-407