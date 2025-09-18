# make_jsp_expr_node_binary

## Location
src/backend/utils/adt/jsonb_gin.c: 395 - 407

## Overview
Creates a binary JSON path GIN expression node with two child arguments for use in GIN index operations on JSONB data.

## Definition


## Detailed Description
This function is a helper utility that constructs a binary expression node in the JSON path GIN index tree structure. It creates a new JsonPathGinNode with exactly two child arguments, which is commonly needed for binary operations like logical AND/OR operations or comparison operations in JSON path expressions. The function serves as a convenience wrapper around  specifically for binary operations.

## Parameters / Member Variables
- : The JsonPathGinNodeType that specifies what kind of binary operation this node represents
- : The first child argument node in the binary expression
- : The second child argument node in the binary expression

## Dependencies
- Functions called/Symbols referenced:
  - make_jsp_expr_node
  - JsonPathGinNodeType
  - JsonPathGinNode
- Called from (representative examples):
  - jsonb_ops__extract_nodes
  - extract_jsp_bool_expr

## Notes and Other Information
- This is a static function within the JSONB GIN indexing module
- Part of the PostgreSQL GIN (Generalized Inverted Index) infrastructure for efficient JSONB querying
- The function assumes exactly 2 arguments and allocates space accordingly
- Located in src/backend/utils/adt/jsonb_gin.c:395-407