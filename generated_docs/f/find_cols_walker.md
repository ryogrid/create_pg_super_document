# find_cols_walker

## Location
[src/backend/executor/nodeAgg.c:1420-1467](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1420-L1467)

## Overview
A tree walker function that recursively traverses expression nodes to identify column references and categorize them as aggregated or unaggregated based on their context within aggregate functions.

## Definition

```c
static bool
find_cols_walker(Node *node, FindColsContext *context)
```
## Detailed Description
The  function is a recursive tree walker that implements the core logic for column reference discovery used by . It traverses expression trees following PostgreSQL's expression_tree_walker pattern and categorizes variable references based on whether they appear within aggregate function expressions or not.

When encountering a Var node (column reference), the function adds the column number to either the aggregated or unaggregated bitmapset depending on the current context. The context tracks whether the walker is currently inside an Aggref node through the  flag. When an Aggref node is encountered, the function sets this flag to true before recursively processing the aggregate's arguments, then resets it to false afterward.

The function ensures that all variable references have been properly processed by setrefs.c by asserting that they use OUTER_VAR as their varno and have varlevelsup of 0.

## Parameters / Member Variables
- `*node`: The expression node being examined during tree traversal
- `*context`: Pointer to FindColsContext structure containing traversal state and result bitmapsets
## Dependencies
- Functions called/Symbols referenced:
  - [bms_add_member](../b/bms_add_member.md)
  - expression_tree_walker
  - [find_cols_walker](find_cols_walker.md) (recursive)
- Types referenced:
  - [Node](../N/Node.md)
  - [FindColsContext](../F/FindColsContext.md)
  - [Var](../V/Var.md)
  - [Aggref](../A/Aggref.md)
  - OUTER_VAR
- Called from (representative examples):
  - [find_cols](find_cols.md)
  - [find_cols_walker](find_cols_walker.md) (recursive calls)

## Notes and Other Information
- Follows PostgreSQL's standard tree walker pattern by returning false to continue traversal
- Uses assertion checks to verify that variable references have been properly processed by the query planner
- The recursive nature allows it to properly handle nested expressions and aggregate functions
- Critical for aggregation optimization as it enables the executor to understand which columns are needed in different phases of aggregation processing
- The context's  flag provides the state needed to correctly categorize column references based on their location relative to aggregate function boundaries

## Simplified Source

```c
static bool find_cols_walker(Node *node, FindColsContext *context) {
    if (node == NULL)
        return false;

    if (IsA(node, Var)) {
        Var *var = (Var *) node;

        // Add column to appropriate set based on context
        if (context->is_aggref)
            context->aggregated = bms_add_member(context->aggregated, var->varattno);
        else
            context->unaggregated = bms_add_member(context->unaggregated, var->varattno);
        return false;
    }

    if (IsA(node, Aggref)) {
        // Process aggregate function arguments
        context->is_aggref = true;
        expression_tree_walker(node, find_cols_walker, (void *) context);
        context->is_aggref = false;
        return false;
    }

    // Continue traversing expression tree
    return expression_tree_walker(node, find_cols_walker, (void *) context);
}
```