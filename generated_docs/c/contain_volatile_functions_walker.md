# contain_volatile_functions_walker

## Location
[src/backend/optimizer/util/clauses.c:550-658](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L550-L658)

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
  - [check_functions_in_node](check_functions_in_node.md)
  - [contain_volatile_functions_checker](contain_volatile_functions_checker.md)
  - query_tree_walker
  - expression_tree_walker
  - VOLATILITY_NOVOLATILE/VOLATILITY_VOLATILE constants
- Called from (representative examples):
  - [contain_volatile_functions](contain_volatile_functions.md)
  - max_parallel_hazard_context (indirectly)

## Notes and Other Information
- Implements short-circuit evaluation, returning `true` immediately upon finding the first volatile function
- Cache invalidation is the responsibility of code that modifies RestrictInfo or PathTarget nodes
- Static function indicating it's an internal implementation detail of the volatility analysis system
- Critical for determining parallel query safety and index optimization decisions

## Simplified Source

```c
static bool
contain_volatile_functions_walker(Node *node, void *context)
{
    if (node == NULL)
        return false;

    // Check functions in current node
    if (check_functions_in_node(node, contain_volatile_functions_checker, context))
        return true;

    // NextValueExpr is always volatile
    if (IsA(node, NextValueExpr))
        return true;

    // Cache results for RestrictInfo nodes
    if (IsA(node, RestrictInfo))
    {
        RestrictInfo *rinfo = (RestrictInfo *) node;

        if (rinfo->has_volatile == VOLATILITY_NOVOLATILE)
            return false;
        else if (rinfo->has_volatile == VOLATILITY_VOLATILE)
            return true;
        else
        {
            // Check and cache volatility
            bool hasvolatile = contain_volatile_functions_walker((Node *) rinfo->clause, context);
            rinfo->has_volatile = hasvolatile ? VOLATILITY_VOLATILE : VOLATILITY_NOVOLATILE;
            return hasvolatile;
        }
    }

    // Cache results for PathTarget nodes
    if (IsA(node, PathTarget))
    {
        PathTarget *target = (PathTarget *) node;

        if (target->has_volatile_expr == VOLATILITY_NOVOLATILE)
            return false;
        else if (target->has_volatile_expr == VOLATILITY_VOLATILE)
            return true;
        else
        {
            // Check and cache volatility
            bool hasvolatile = contain_volatile_functions_walker((Node *) target->exprs, context);
            target->has_volatile_expr = hasvolatile ? VOLATILITY_VOLATILE : VOLATILITY_NOVOLATILE;
            return hasvolatile;
        }
    }

    // Handle subqueries
    if (IsA(node, Query))
        return query_tree_walker((Query *) node, contain_volatile_functions_walker, context, 0);

    // Continue recursive traversal
    return expression_tree_walker(node, contain_volatile_functions_walker, context);
}
```