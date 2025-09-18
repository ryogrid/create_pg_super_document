# make_jsp_expr_node_args

## Location
[src/backend/utils/adt/jsonb_gin.c:382-394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L382-L394)

## Overview
Creates a JsonPathGinNode expression with initialized arguments by combining node creation with argument population from a PostgreSQL List.

## Definition


## Detailed Description
This function provides a complete solution for creating expression nodes with pre-populated arguments. It first creates a JsonPathGinNode using make_jsp_expr_node() with the appropriate size based on the list length, then iterates through the provided List to populate each argument slot in the node's args array.

The function uses PostgreSQL's List data structure and associated macros (foreach, lfirst) to traverse the arguments. Each element from the list is directly assigned to the corresponding position in the node's argument array, creating a fully initialized expression node ready for use in JSONB GIN index operations.

## Parameters / Member Variables
- : JsonPathGinNodeType enum value specifying the type of expression node to create
- : PostgreSQL List containing the argument nodes to be stored in the expression node

## Dependencies
- Functions called/Symbols referenced:
  - [make_jsp_expr_node](make_jsp_expr_node.md) (creates the base expression node structure)
  - list_length (PostgreSQL function to get List length)
  - foreach (PostgreSQL macro for List iteration)
  - lfirst (PostgreSQL macro to extract List cell content)
  - JsonPathGinNodeType (enum for node types)
  - JsonPathGinNode (main node structure)
  - [List](../L/List.md) and ListCell (PostgreSQL List data structures)

- Called from:
  - [extract_jsp_path_expr](../e/extract_jsp_path_expr.md) (extracts JSON path expressions for indexing)

## Notes and Other Information
This function represents a higher-level convenience wrapper that combines node creation with argument initialization, making it easier to create complex expression trees in the JSONB GIN indexing system. The function assumes that all elements in the args List are valid JsonPathGinNode pointers.