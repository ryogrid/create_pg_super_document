# contain_vars_of_level_walker

## Location
[src/backend/optimizer/util/var.c:452-508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L452-L508)

## Overview
A static tree walker function that recursively traverses an expression tree to detect whether it contains any Var nodes, PlaceHolderVar nodes, or CurrentOfExpr nodes at a specified query nesting level.

## Definition
```c
static bool contain_vars_of_level_walker(Node *node, int *sublevels_up)
```

## Detailed Description
This function implements the core traversal logic for `contain_vars_of_level()`, using the PostgreSQL tree walker pattern to search for variable references at a specific query nesting level. The function handles different node types that represent variable references:

- **Var nodes**: Checks if `varlevelsup` matches the target level
- **PlaceHolderVar nodes**: Checks if `phlevelsup` matches the target level, then continues traversing the contained expression
- **CurrentOfExpr nodes**: Only matches at level 0 (current query level)
- **Query nodes**: Recursively processes subqueries by incrementing the nesting level

The function uses the short-circuit evaluation pattern typical of PostgreSQL tree walkers - it returns true immediately when a match is found, aborting further traversal.

## Parameters / Member Variables
- `node`: The current node being examined in the expression tree traversal
- `sublevels_up`: Pointer to an integer tracking the target query nesting level to search for

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - query_tree_walker (for recursing into Query nodes)
  - expression_tree_walker (for recursing into expression nodes)
- Referenced node types:
  - [Var](../V/Var.md)
  - [CurrentOfExpr](../C/CurrentOfExpr.md)
  - [PlaceHolderVar](../P/PlaceHolderVar.md)
  - [Query](../Q/Query.md)
- Called from:
  - [contain_vars_of_level](contain_vars_of_level.md) (main entry point)
  - [contain_vars_of_level_walker](contain_vars_of_level_walker.md) (recursive self-calls)

## Notes and Other Information
- This is a static helper function, not part of the public API
- Implements the standard PostgreSQL tree walker pattern for short-circuit boolean searches
- The function carefully manages nesting levels when encountering subqueries, incrementing before recursion and decrementing after
- [PlaceHolderVar](../P/PlaceHolderVar.md) handling is special: it first checks the placeholder level, then continues traversing the contained expression even if the level matches
- [CurrentOfExpr](../C/CurrentOfExpr.md) nodes are only considered to match at the outermost query level (sublevels_up == 0)

## Simplified Source

```c
static bool contain_vars_of_level_walker(Node *node, int *sublevels_up) {
    if (node == NULL)
        return false;

    // Check for Var nodes at target level
    if (IsA(node, Var)) {
        if (((Var *) node)->varlevelsup == *sublevels_up)
            return true; // Found match, abort traversal
        return false;
    }

    // Check for CurrentOfExpr at outermost level only
    if (IsA(node, CurrentOfExpr)) {
        if (*sublevels_up == 0)
            return true;
        return false;
    }

    // Check for PlaceHolderVar at target level
    if (IsA(node, PlaceHolderVar)) {
        if (((PlaceHolderVar *) node)->phlevelsup == *sublevels_up)
            return true; // Found match, abort traversal
        // Continue checking contained expression
    }

    // Handle subqueries by adjusting nesting level
    if (IsA(node, Query)) {
        bool result;

        (*sublevels_up)++; // Descend into subquery
        result = query_tree_walker((Query *) node,
                                 contain_vars_of_level_walker,
                                 (void *) sublevels_up, 0);
        (*sublevels_up)--; // Restore level
        return result;
    }

    // Recursively check all other expression nodes
    return expression_tree_walker(node,
                                contain_vars_of_level_walker,
                                (void *) sublevels_up);
}
```