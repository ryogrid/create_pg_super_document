# JsonArgument

## Location
[src/include/nodes/parsenodes.h:1762-1767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1762-L1767)

## Overview
A structure representing named arguments from the JSON PASSING clause in SQL/JSON expressions, providing parameter binding for JSON operations.

## Definition

```c
typedef struct JsonArgument
{
	NodeTag		type;
	JsonValueExpr *val;			/* argument value expression */
	char	   *name;			/* argument name */
} JsonArgument;
```
## Detailed Description
JsonArgument represents individual named arguments from the PASSING clause in SQL/JSON expressions. The PASSING clause allows users to bind external values to named parameters that can be referenced within JSON path expressions and other JSON operations. This structure encapsulates both the parameter name and its associated value expression, enabling dynamic parameter substitution in JSON processing. JsonArgument serves as a bridge between SQL expressions and JSON path contexts, allowing complex JSON operations to access external data through named parameter bindings.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL node identification
- : Pointer to JsonValueExpr representing the argument's value expression
- : String containing the name of the argument parameter

## Dependencies
- Functions called/Symbols referenced:
  - [JsonValueExpr](JsonValueExpr.md)
- Called from (representative examples):
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md)
  - [transformJsonPassingArgs](../t/transformJsonPassingArgs.md)

## Notes and Other Information
- Essential component of SQL/JSON's PASSING clause mechanism for parameter binding
- Enables dynamic value injection into JSON path expressions and JSON operations
- The name field provides the identifier that can be referenced within JSON contexts
- Works in conjunction with JsonValueExpr to provide complete argument specification
- Used throughout PostgreSQL's JSON processing pipeline to maintain parameter context
- Located in src/include/nodes/parsenodes.h at lines 1762-1767