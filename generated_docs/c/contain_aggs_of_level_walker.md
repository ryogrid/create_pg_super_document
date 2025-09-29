# contain_aggs_of_level_walker

## Location
[src/backend/rewrite/rewriteManip.c:103-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L103-L149)

## Overview
A recursive tree walker callback function that examines nodes to detect aggregate functions (Aggref and GroupingFunc) at a specific query nesting level.

## Definition
```c
static bool contain_aggs_of_level_walker(Node *node, contain_aggs_of_level_context *context)
```

## Detailed Description
This static function serves as the core implementation for the contain_aggs_of_level functionality. It operates as a callback for PostgreSQL's tree walking infrastructure, recursively examining each node in an expression tree to identify aggregate functions that belong to the target query level.

The function handles multiple node types with specific logic:
- For Aggref nodes (regular aggregates): Checks if the agglevelsup matches the target level
- For GroupingFunc nodes (GROUPING() function): Similar level checking as Aggref
- For Query nodes (subselects): Recursively descends while adjusting the level context
- For other nodes: Continues tree traversal using expression_tree_walker

The level tracking mechanism ensures that aggregates are correctly attributed to their proper query scope, which is essential for query optimization and rewriting operations.

## Parameters / Member Variables
- `node`: The current node being examined in the tree traversal
- `context`: Context structure containing the target sublevels_up value for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [contain_aggs_of_level_context](contain_aggs_of_level_context.md) (context structure)
  - [Aggref](../A/Aggref.md) (aggregate function reference node)
  - [GroupingFunc](../G/GroupingFunc.md) (GROUPING function node)
  - query_tree_walker (subquery traversal)
  - expression_tree_walker (general expression traversal)
- Called from (representative examples):
  - [contain_aggs_of_level](contain_aggs_of_level.md) (main entry point)
  - [contain_aggs_of_level_walker](contain_aggs_of_level_walker.md) (recursive self-calls)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:103-149
- Static function, only accessible within the same compilation unit
- Uses PostgreSQL's standard tree walker pattern for robust node traversal
- Handles proper level adjustment when recursing into subqueries
- Returns true immediately upon finding a matching aggregate (short-circuit evaluation)
- Critical for maintaining correct aggregate scoping in nested query structures

## Simplified Source

```c
static bool contain_aggs_of_level_walker(Node *node, contain_aggs_of_level_context *context) {
    if (node == NULL)
        return false;

    // Check for aggregate functions at the target level
    if (IsA(node, Aggref)) {
        if (((Aggref *) node)->agglevelsup == context->sublevels_up)
            return true;  // Found matching aggregate
        // Continue searching through aggregate's arguments
    }

    // Check for GROUPING functions at the target level
    if (IsA(node, GroupingFunc)) {
        if (((GroupingFunc *) node)->agglevelsup == context->sublevels_up)
            return true;  // Found matching grouping function
        // Continue searching through function's arguments
    }

    // Handle subqueries with proper level adjustment
    if (IsA(node, Query)) {
        bool result;

        // Increment level for subquery recursion
        context->sublevels_up++;
        result = query_tree_walker((Query *) node,
                                 contain_aggs_of_level_walker,
                                 context, 0);
        // Restore original level
        context->sublevels_up--;

        return result;
    }

    // Continue searching through other expression nodes
    return expression_tree_walker(node, contain_aggs_of_level_walker, context);
}
```