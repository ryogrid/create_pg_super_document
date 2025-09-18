# contain_volatile_functions_walker

## Location
src/backend/optimizer/util/clauses.c: 550 - 658

## Overview
A recursive tree walker that traverses expression trees to detect volatile functions, implementing sophisticated caching mechanisms for performance optimization in PostgreSQL's query planner.

## Definition
```c
static bool contain_volatile_functions_walker(Node *node, void *context)
```

## Detailed Description
This function serves as the core implementation for volatile function detection in PostgreSQL expressions. It employs several optimization strategies and handles special node types:

**Caching Strategy**: 
- **RestrictInfo nodes**: Caches volatility results in `has_volatile` field using VOLATILITY_UNKNOWN/NOVOLATILE/VOLATILE states
- **PathTarget nodes**: Similar caching in `has_volatile_expr` field to avoid redundant analysis of target expressions

**Special Node Handling**:
- **NextValueExpr**: Immediately returns true as sequence operations are inherently volatile
- **Query nodes**: Recursively processes subselects using `query_tree_walker`
- **Function calls**: Uses `check_functions_in_node` with `contain_volatile_functions_checker` to examine function volatility

**Performance Considerations**: The caching mechanism significantly improves performance for complex queries by avoiding repeated volatility analysis of the same expressions, particularly important for RestrictInfo clauses that are examined multiple times during planning.

The function deliberately excludes certain node types (MinMaxExpr, XmlExpr, CoerceToDomain, SQLValueFunction) from volatility checks, following the same rationale as the mutable functions walker.

## Parameters / Member Variables
- `node`: The expression tree node being examined for volatile functions
- `context`: Opaque context pointer passed through the traversal (typically unused)

## Dependencies
- Functions called/Symbols referenced:
  - check_functions_in_node
  - contain_volatile_functions_checker
  - query_tree_walker
  - expression_tree_walker
  - VOLATILITY_NOVOLATILE/VOLATILITY_VOLATILE constants
- Called from (representative examples):
  - contain_volatile_functions
  - max_parallel_hazard_context (indirectly)

## Notes and Other Information
- Implements short-circuit evaluation, returning `true` immediately upon finding the first volatile function
- Cache invalidation is the responsibility of code that modifies RestrictInfo or PathTarget nodes
- Static function indicating it's an internal implementation detail of the volatility analysis system
- Critical for determining parallel query safety and index optimization decisions