# make_jsp_entry_node

## Location
[src/backend/utils/adt/jsonb_gin.c:353-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L353-L363)

## Overview
Creates a JsonPathGinNode structure to represent a simple entry datum in the JSONB GIN indexing system.

## Definition

```c
static JsonPathGinNode *
make_jsp_entry_node(Datum entry)
```
## Detailed Description
This function is a constructor utility that creates a JsonPathGinNode with type JSP_GIN_ENTRY to encapsulate a Datum value for use in JSONB GIN index operations. The function allocates memory for the base JsonPathGinNode structure without the args array (using offsetof to calculate the exact size needed), initializes the node type to JSP_GIN_ENTRY, and stores the provided Datum in the entryDatum field.

This is a fundamental building block in the JSONB GIN indexing system, used to create nodes that represent individual indexable values extracted from JSONB documents.

## Parameters / Member Variables
- : The Datum value to be stored in the GIN node, representing an indexable entry extracted from JSONB data

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - offsetof (C macro to calculate structure member offset)
  - [JsonPathGinNode](../J/JsonPathGinNode.md) (GIN node structure type)
  - JSP_GIN_ENTRY (constant defining the entry node type)

- Called from:
  - [make_jsp_entry_node_scalar](make_jsp_entry_node_scalar.md) (creates entry nodes for scalar values)
  - [jsonb_ops__extract_nodes](../j/jsonb_ops__extract_nodes.md) (extracts indexable nodes for jsonb_ops operator class)
  - [jsonb_path_ops__extract_nodes](../j/jsonb_path_ops__extract_nodes.md) (extracts indexable nodes for jsonb_path_ops operator class)

## Notes and Other Information
The function uses offsetof(JsonPathGinNode, args) to allocate only the necessary memory for the base structure without the variable-length args array, which is not needed for simple entry nodes. This is a memory optimization for nodes that don't require additional arguments.