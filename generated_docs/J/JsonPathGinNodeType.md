# JsonPathGinNodeType

## Location
[src/backend/utils/adt/jsonb_gin.c:92-93](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L92-L93)

## Overview
An enumeration type that defines the different node types used in the internal representation of JSON path expressions for GIN (Generalized Inverted Index) operations.

## Definition

```c
typedef struct JsonPathGinNode JsonPathGinNode;
```
## Detailed Description
JsonPathGinNodeType is an enumeration that categorizes nodes in the internal tree representation of JSON path expressions when processed for GIN indexing. This type system enables PostgreSQL to efficiently represent and process complex JSON path queries by breaking them down into logical operations (OR, AND) and terminal entry nodes. The enumeration is fundamental to the GIN index support for JSON path operations, allowing the system to optimize query execution by understanding the logical structure of path expressions.

## Parameters / Member Variables
- : Represents logical OR operations in JSON path expressions (||)
- : Represents logical AND operations in JSON path expressions (&&) 
- : Represents terminal entry nodes containing actual path components or values

## Dependencies
- Functions called/Symbols referenced:
  - (This is an enum type, so it doesn't call other functions)
- Called from (representative examples):
  - [make_jsp_expr_node](../m/make_jsp_expr_node.md) (src/backend/utils/adt/jsonb_gin.c:370)
  - [make_jsp_expr_node_args](../m/make_jsp_expr_node_args.md) (src/backend/utils/adt/jsonb_gin.c:382)
  - [make_jsp_expr_node_binary](../m/make_jsp_expr_node_binary.md) (src/backend/utils/adt/jsonb_gin.c:395)
  - [extract_jsp_bool_expr](../e/extract_jsp_bool_expr.md) (src/backend/utils/adt/jsonb_gin.c:596)

## Notes and Other Information
- This enum is used as the  field in JsonPathGinNode structures to identify the node's role in the expression tree
- The enum supports the logical operators found in JSON path boolean expressions
- JSP_GIN_ENTRY nodes can contain either entry indices (for processed entries) or entry datums (for unprocessed entries) depending on the processing stage
- This type system is essential for the GIN index's ability to efficiently search JSON documents using path expressions