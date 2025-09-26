# JsonPathGinNode

## Location
[src/backend/utils/adt/jsonb_gin.c:94-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L94-L97)

## Overview
JsonPathGinNode represents a node in an expression tree structure used for processing JSON path queries in GIN index operations, supporting logical operations (AND/OR) and entry matching for JSONB data.

## Definition

```c
typedef struct JsonPathGinNode JsonPathGinNode;
```
## Detailed Description
JsonPathGinNode implements a flexible tree structure for representing JSON path query expressions in a form suitable for GIN index processing. The structure supports three types of nodes defined by JsonPathGinNodeType: JSP_GIN_OR (logical OR), JSP_GIN_AND (logical AND), and JSP_GIN_ENTRY (leaf entry nodes).

The node uses a union to store different types of data depending on its type:
- For OR/AND nodes: stores the number of child arguments (nargs)
- For ENTRY nodes: can store either a Datum value (before processing) or an index into a GinEntries array (after processing)

The flexible array member 'args' allows OR and AND nodes to have a variable number of children, creating a tree structure that can represent complex boolean expressions over JSON path conditions.

This structure is central to the GIN indexing strategy for JSONB operations, converting JSON path queries into efficient tree representations that can be evaluated against indexed data.

## Parameters / Member Variables
- : Specifies the node type (JSP_GIN_OR, JSP_GIN_AND, or JSP_GIN_ENTRY)
- : Number of child arguments for OR and AND nodes
- : Index into GinEntries array for processed ENTRY nodes
- : Raw datum value (path hash or key/scalar) for unprocessed ENTRY nodes
- : Variable-length array of pointers to child nodes for OR and AND nodes

## Dependencies
- Functions called/Symbols referenced:
  - [JsonPathGinNodeType](JsonPathGinNodeType.md) enum
  - Datum (PostgreSQL data type)
  - FLEXIBLE_ARRAY_MEMBER (PostgreSQL macro)
- Called from (representative examples):
  - [make_jsp_entry_node](../m/make_jsp_entry_node.md) (creates entry nodes)
  - [make_jsp_expr_node](../m/make_jsp_expr_node.md) (creates OR/AND nodes)
  - [extract_jsp_bool_expr](../e/extract_jsp_bool_expr.md) (expression extraction)
  - [execute_jsp_gin_node](../e/execute_jsp_gin_node.md) (node execution)
  - [gin_consistent_jsonb](../g/gin_consistent_jsonb.md) (consistency checking)
  - [gin_triconsistent_jsonb](../g/gin_triconsistent_jsonb.md) (tri-state consistency)

## Notes and Other Information
- Memory allocation uses offsetof calculation to accommodate the flexible array member
- Entry nodes are allocated without space for args array since they are leaf nodes
- OR/AND nodes pre-allocate space for their specified number of child arguments
- The structure supports both pre-processed (entryDatum) and post-processed (entryIndex) states for entry nodes
- Tree evaluation follows standard boolean logic for OR and AND operations
- Used extensively in JSON path query optimization and execution within GIN indexes