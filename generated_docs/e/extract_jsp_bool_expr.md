# extract_jsp_bool_expr

## Location
[src/backend/utils/adt/jsonb_gin.c:583-718](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L583-L718)

## Overview
Recursively extracts and builds a GIN-compatible query tree from boolean jsonpath expressions, handling logical operations (AND, OR, NOT), existence checks, and equality comparisons with proper negation support.

## Definition

```c
static JsonPathGinNode *
extract_jsp_bool_expr(JsonPathGinContext *cxt, JsonPathGinPath path,
					  JsonPathItem *jsp, bool not)
```
## Detailed Description
This function serves as the core boolean expression processor for jsonpath GIN index queries. It recursively traverses a jsonpath boolean expression tree and converts it into a  structure that can be efficiently processed by PostgreSQL's GIN indexing system. The function handles De Morgan's law transformations when the  parameter is true, converting AND operations to OR and vice versa.

The function supports:
- Logical AND () and OR () operations with proper boolean algebra
- NOT () operations with negation propagation
- EXISTS predicates (when not negated)
- Equality comparisons () between paths and scalar values
- Proper handling of scalar types (null, boolean, numeric, string)

For unsupported operations like NOT EXISTS or inequality comparisons with negation, the function returns NULL, indicating the query cannot be optimized using GIN indexes.

## Parameters / Member Variables
- : JsonPathGinContext containing extraction state and configuration
- : Current JsonPathGinPath representing the path context for extraction
- : JsonPathItem pointer to the current boolean expression node being processed
- : Boolean flag indicating whether the current expression should be logically negated

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - [jspGetLeftArg](../j/jspGetLeftArg.md), jspGetRightArg, jspGetArg (jsonpath item accessors)
  - [make_jsp_expr_node_binary](../m/make_jsp_expr_node_binary.md) (creates binary expression nodes)
  - [extract_jsp_path_expr](extract_jsp_path_expr.md) (processes path expressions)
  - jspIsScalar (checks if item is a scalar value)
- Called from (representative examples):
  - [extract_jsp_path_expr_nodes](extract_jsp_path_expr_nodes.md) (main extraction entry point)
  - [extract_jsp_bool_expr](extract_jsp_bool_expr.md) (recursive self-calls for nested expressions)
  - [extract_jsp_query](extract_jsp_query.md) (top-level query extraction)

## Notes and Other Information
- The function implements proper boolean algebra with De Morgan's law support for negations
- NOT EXISTS operations are explicitly not supported and return NULL
- Inequality comparisons with negation are not supported due to semantic complexity with JSON sequence comparisons
- The function uses recursive descent parsing with stack depth checking to prevent overflow
- Scalar value extraction handles all JSON scalar types: null, boolean, numeric, and string
- Returns NULL for unsupported operations, allowing the query planner to fall back to sequential scans