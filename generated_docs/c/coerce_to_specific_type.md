# coerce_to_specific_type

## Location
[src/backend/parser/parse_coerce.c:1257-1272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L1257-L1272)

## Overview
A convenience function that coerces an expression node to a specific data type for SQL constructs that require a particular type, ensuring the input is not a set-returning expression.

## Definition

```c
Node *
coerce_to_specific_type(ParseState *pstate, Node *node,
						Oid targetTypeId,
						const char *constructName)
```
## Detailed Description
This function serves as a simplified wrapper around , providing type coercion functionality for SQL constructs that need arguments of a specific data type. It automatically uses a default typmod of -1 (meaning no specific type modifier is required) and delegates the actual coercion work to the more general  function.

The function is commonly used throughout the PostgreSQL parser when processing SQL constructs like LIMIT clauses, XML expressions, JSON functions, and range table functions that have strict type requirements for their arguments.

## Parameters / Member Variables
- : ParseState pointer for error reporting and context; may be NULL if no special unknown-Param processing is needed
- : The input expression node to be coerced
- : The OID of the target data type to coerce to
- : Name of the SQL construct (for error messages), e.g., "LIMIT", "XMLELEMENT"

## Dependencies
- Functions called/Symbols referenced:
  - [coerce_to_specific_type_typmod](coerce_to_specific_type_typmod.md)
- Called from (representative examples):
  - [interpret_function_parameter_list](../i/interpret_function_parameter_list.md) (src/backend/commands/functioncmds.c:409)
  - [transformRangeTableFunc](../t/transformRangeTableFunc.md) (src/backend/parser/parse_clause.c:720, 728, 790, 848)
  - [transformRangeTableSample](../t/transformRangeTableSample.md) (src/backend/parser/parse_clause.c:977, 996)
  - [transformLimitClause](../t/transformLimitClause.md) (src/backend/parser/parse_clause.c:1892)
  - [transformXmlExpr](../t/transformXmlExpr.md) (src/backend/parser/parse_expr.c:2435, 2442, 2447, 2453, 2458, 2461, 2464, 2472)
  - [transformJsonValueExpr](../t/transformJsonValueExpr.md) (src/backend/parser/parse_expr.c:3301)

## Notes and Other Information
- This is a thin wrapper function that provides a simpler interface when no specific type modifier is needed
- The actual type coercion logic, including error handling for incompatible types and set-returning expressions, is implemented in 
- Widely used across the parser for enforcing type constraints in various SQL constructs
- Part of PostgreSQL's type coercion system located in src/backend/parser/parse_coerce.c

## Simplified Source

```c
Node *
coerce_to_specific_type(ParseState *pstate, Node *node,
                       Oid targetTypeId,
                       const char *constructName) {
    // Delegate to the full function with default typmod (-1)
    return coerce_to_specific_type_typmod(pstate, node,
                                        targetTypeId, -1,
                                        constructName);
}
```