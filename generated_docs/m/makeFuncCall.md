# makeFuncCall

## Location
[src/backend/nodes/makefuncs.c:650-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L650-L674)

## Overview
Creates a FuncCall node representing function calls in PostgreSQL's parse tree, initializing all essential fields with sensible defaults.

## Definition
```c
FuncCall *makeFuncCall(List *name, List *args, CoercionForm funcformat, int location)
```

## Detailed Description
The makeFuncCall function constructs a FuncCall node, which represents function calls in SQL queries and expressions. This constructor initializes the FuncCall structure with the essential parameters that every function call must have, while setting optional fields to their default values. The function handles regular function calls, and the caller can later modify specific fields for specialized cases like aggregate functions, window functions, or functions with special syntax. All aggregate-related fields are initialized to their default (disabled) state, and window function fields are set to NULL.

## Parameters / Member Variables
- `name`: List of strings representing the qualified function name (e.g., ["schema", "function_name"] for schema.function_name)
- `args`: List of Node pointers representing the function arguments (can be NIL for functions without arguments)
- `funcformat`: CoercionForm enum value indicating the format/syntax used for the function call (e.g., COERCE_EXPLICIT_CALL, COERCE_SQL_SYNTAX)
- `location`: Integer representing the location in the source query where this function call appears

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to allocate FuncCall node)
  - [FuncCall](../F/FuncCall.md) (the node structure being created)
  - CoercionForm (enum type for the funcformat parameter)
  - NIL (empty list constant)
- Called from (representative examples):
  - [transformRangeFunction](../t/transformRangeFunction.md)
  - [transformColumnDefinition](../t/transformColumnDefinition.md)
  - [test_rls_hooks_permissive](../t/test_rls_hooks_permissive.md)
  - [test_rls_hooks_restrictive](../t/test_rls_hooks_restrictive.md)

## Notes and Other Information
- All aggregate-related fields (agg_order, agg_filter, agg_within_group, agg_star, agg_distinct) are initialized to their default disabled state
- Window function fields (over) are set to NULL initially
- The func_variadic field is set to false by default
- Callers must modify specific fields after creation for specialized function call types (aggregates, window functions, etc.)
- The function name is stored as a List to support qualified names with schema specifications
- This is the standard constructor for most function call scenarios in query parsing and transformation