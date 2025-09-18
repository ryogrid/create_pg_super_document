# contain_mutable_functions_walker

## Location
[src/backend/optimizer/util/clauses.c:382-489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L382-L489)

## Overview
A recursive walker function that traverses expression trees to detect mutable (non-immutable) functions, supporting PostgreSQL's query optimization by identifying expressions that cannot be treated as constants.

## Definition
```c
static bool contain_mutable_functions_walker(Node *node, void *context)
```

## Detailed Description
This function performs a depth-first traversal of expression trees to identify mutable functions and expressions. It serves as the core implementation for PostgreSQL's mutability analysis, which is crucial for query optimization decisions such as constant folding, index usage, and parallel query execution.

The function handles several special cases:
- **JsonConstructorExpr**: Checks if JSON/JSONB conversion functions are immutable based on argument types
- **JsonExpr**: Validates JSON path expressions for mutability using `jspIsMutable`
- **SQLValueFunction**: Treats all SQL value functions (CURRENT_TIMESTAMP, etc.) as stable/mutable
- **NextValueExpr**: Treats sequence operations as volatile
- **Query nodes**: Recursively processes subselects

The walker uses `check_functions_in_node` with `contain_mutable_functions_checker` to examine function calls, checking their volatility category against PROVOLATILE_IMMUTABLE.

## Parameters / Member Variables
- `node`: The expression tree node being examined for mutable functions
- `context`: Opaque context pointer passed through the traversal (typically unused)

## Dependencies
- Functions called/Symbols referenced:
  - check_functions_in_node
  - [contain_mutable_functions_checker](contain_mutable_functions_checker.md)  
  - [to_jsonb_is_immutable](../t/to_jsonb_is_immutable.md)
  - [to_json_is_immutable](../t/to_json_is_immutable.md)
  - [jspIsMutable](../j/jspIsMutable.md)
  - query_tree_walker
  - expression_tree_walker
- Called from (representative examples):
  - [contain_mutable_functions](contain_mutable_functions.md)
  - max_parallel_hazard_context (indirectly)

## Notes and Other Information
- Returns `true` immediately upon finding the first mutable function (short-circuit evaluation)
- Handles PostgreSQL-specific expression types like JSON operations and SQL value functions
- Part of the broader volatility analysis framework used throughout the query optimizer
- The function is marked static, indicating it's an internal implementation detail of clauses.c