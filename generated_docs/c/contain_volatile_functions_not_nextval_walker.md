# contain_volatile_functions_not_nextval_walker

## Location
src/backend/optimizer/util/clauses.c: 686 - 733

## Overview
A static tree walker function that recursively traverses expression trees to detect volatile functions while specifically ignoring nextval() calls.

## Definition
```c
static bool contain_volatile_functions_not_nextval_walker(Node *node, void *context)
```

## Detailed Description
This function implements a specialized tree walker that performs recursive traversal of PostgreSQL expression trees to detect volatile functions with special handling for nextval(). The walker is specifically designed for COPY operations where nextval() should not be considered a blocking volatile function.

Key aspects of the implementation:

1. **Function checking**: Uses `check_functions_in_node()` with the specialized `contain_volatile_functions_not_nextval_checker()` to test volatility in the current node
2. **Special node handling**: Intentionally ignores certain expression types (MinMaxExpr, XmlExpr, CoerceToDomain, SQLValueFunction, NextValueExpr) that are treated as immutable or stable in this context
3. **Recursive traversal**: Uses appropriate walker functions for different node types:
   - `query_tree_walker()` for Query nodes (subselects)
   - `expression_tree_walker()` for expression nodes

The function follows the standard PostgreSQL tree walker pattern, returning true immediately when a volatile function is found, or false if the entire tree contains no volatile functions (excluding nextval).

## Parameters / Member Variables
- `node`: The current node in the expression tree being examined
- `context`: Context information passed through the walker (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - `check_functions_in_node`: Checks functions in the current node
  - `contain_volatile_functions_not_nextval_checker`: Specialized volatility checker function
  - `query_tree_walker`: Handles recursive traversal of Query nodes
  - `expression_tree_walker`: Handles recursive traversal of expression nodes
- Called from (representative examples):
  - `max_parallel_hazard_context` (at clauses.c:100)
  - `contain_volatile_functions_not_nextval` (at clauses.c:675)
  - Recursive self-calls (at clauses.c:709, 713)

## Notes and Other Information
- This is a static function, only visible within the clauses.c compilation unit
- Implements the PostgreSQL tree walker pattern for systematic tree traversal
- The special handling of NextValueExpr is consistent with ignoring nextval() functions
- Designed specifically for COPY operation requirements where nextval() has different semantics
- Returns true on first volatile function found (short-circuit evaluation)
- Part of the infrastructure supporting parallel-safe analysis for bulk operations