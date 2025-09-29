# expression_returns_set_walker

## Location
[src/backend/nodes/nodeFuncs.c:764-815](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L764-L815)

## Overview
A tree walker function that recursively examines expression nodes to detect set-returning functions and operators within an expression tree.

## Definition
```c
static bool expression_returns_set_walker(Node *node, void *context)
```

## Detailed Description
This static function implements the core logic for detecting set-returning expressions by walking through expression trees recursively. It specifically checks for FuncExpr nodes with the funcretset flag and OpExpr nodes with the opretset flag, which indicate functions and operators that return sets rather than single values. The function includes optimizations to avoid unnecessary recursion for certain node types (Aggref, GroupingFunc, WindowFunc) that are known by the parser to not return sets. It uses PostgreSQL's expression_tree_walker framework for efficient tree traversal and calls itself recursively to examine sub-expressions.

## Parameters / Member Variables
- `node`: The current expression node being examined for set-returning behavior
- `context`: Additional context passed through the tree walker (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for node type checking)
  - [FuncExpr](../F/FuncExpr.md) (function call expression node)
  - [OpExpr](../O/OpExpr.md) (operator expression node)
  - [Aggref](../A/Aggref.md) (aggregate function reference node)
  - [GroupingFunc](../G/GroupingFunc.md) (grouping function node)
  - [WindowFunc](../W/WindowFunc.md) (window function node)
  - expression_tree_walker (framework function for tree traversal)

- Called from (representative examples):
  - [expression_returns_set](expression_returns_set.md) (public wrapper function)
  - Self-recursive calls during tree traversal

## Notes and Other Information
- The function is declared static, meaning it's only accessible within the nodeFuncs.c file
- Contains explicit optimizations to skip recursion for node types guaranteed not to return sets
- Comments indicate that changes to this function should be coordinated with expression_returns_set_rows() in clauses.c and IS_SRF_CALL() in tlist.c
- The funcretset and opretset flags are set during function/operator lookup and indicate the return type characteristics
- Part of PostgreSQL's broader framework for handling set-returning functions (SRFs) which require special execution strategies
- Returns true as soon as any set-returning expression is found, providing early termination for efficiency

## Simplified Source

```c
static bool
expression_returns_set_walker(Node *node, void *context)
{
    if (node == NULL)
        return false;

    // Check function expressions
    if (IsA(node, FuncExpr))
    {
        FuncExpr *expr = (FuncExpr *) node;
        if (expr->funcretset)
            return true;  // Found set-returning function
    }

    // Check operator expressions
    if (IsA(node, OpExpr))
    {
        OpExpr *expr = (OpExpr *) node;
        if (expr->opretset)
            return true;  // Found set-returning operator
    }

    // Skip nodes known not to return sets
    if (IsA(node, Aggref) || IsA(node, GroupingFunc) || IsA(node, WindowFunc))
        return false;

    // Continue recursive traversal
    return expression_tree_walker(node, expression_returns_set_walker, context);
}
```