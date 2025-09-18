# make_jsp_entry_node_scalar

## Location
[src/backend/utils/adt/jsonb_gin.c:364-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L364-L369)

## Overview
Creates a JsonPathGinNode for scalar JSONB values by combining scalar key generation with entry node creation.

## Definition


## Detailed Description
This function serves as a convenience wrapper that combines two operations: converting a JsonbValue scalar into a searchable key format using make_scalar_key(), then wrapping that key in a JsonPathGinNode using make_jsp_entry_node(). The function handles both regular values and key values, with the iskey parameter controlling how the scalar is processed for indexing.

This is commonly used in JSONB GIN index extraction to create indexable nodes from scalar values found within JSONB documents, ensuring they are properly formatted for efficient searching.

## Parameters / Member Variables
- : Pointer to JsonbValue containing the scalar value to be indexed (string, number, boolean, or null)
- : Boolean flag indicating whether this scalar represents a JSON object key (true) or a value (false)

## Dependencies
- Functions called/Symbols referenced:
  - [make_jsp_entry_node](make_jsp_entry_node.md) (creates the JsonPathGinNode wrapper)
  - [make_scalar_key](make_scalar_key.md) (converts JsonbValue to indexable Datum format)
  - JsonPathGinNode (return type structure)

- Called from:
  - [jsonb_ops__extract_nodes](../j/jsonb_ops__extract_nodes.md) (multiple calls for extracting various scalar types during index creation)

## Notes and Other Information
The iskey parameter is crucial as it affects how the scalar value is processed by make_scalar_key - keys and values may be indexed differently to support various query patterns in JSONB GIN indexes.