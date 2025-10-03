# query_contains_extern_params_walker

## Location
[src/backend/parser/parse_param.c:338-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_param.c#L338-L359)

## Overview
A tree-walking function that searches for PARAM_EXTERN parameters within a query tree, returning true immediately upon finding the first external parameter.

## Definition

```c
static bool
query_contains_extern_params_walker(Node *node, void *context)
```
## Detailed Description
This static function implements the actual tree traversal logic for detecting external parameters in a query tree. It follows the standard PostgreSQL walker pattern, examining each node to determine if it contains a Param node with paramkind set to PARAM_EXTERN.

The function operates efficiently by returning true immediately upon finding the first external parameter, making it an early-terminating search. For Query nodes, it recursively calls query_tree_walker to examine subqueries, and for other expression nodes, it uses expression_tree_walker to continue the traversal.

## Parameters / Member Variables
- `*node`: Current node being examined in the tree traversal
- `*context`: Walker context (unused in this implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [Param](../P/Param.md)
  - PARAM_EXTERN
  - query_tree_walker
  - expression_tree_walker
  - [query_contains_extern_params_walker](query_contains_extern_params_walker.md) (recursive)
- Called from (representative examples):
  - [query_contains_extern_params](query_contains_extern_params.md)
  - [query_contains_extern_params_walker](query_contains_extern_params_walker.md) (recursive)

## Notes and Other Information
- This is a static function used internally for parameter detection
- Located in src/backend/parser/parse_param.c:338-359
- Returns true immediately upon finding any PARAM_EXTERN parameter (early termination)
- Returns false to continue searching if the current node is not an external parameter
- Handles both Query substructures and regular expression trees through recursive calls
- The context parameter is not used but maintained for walker function signature compatibility
- Part of PostgreSQL's parameter analysis infrastructure

## Simplified Source

```c
static bool
query_contains_extern_params_walker(Node *node, void *context)
{
    if (node == NULL)
        return false;

    // Check for external parameter
    if (IsA(node, Param)) {
        Param *param = (Param *) node;
        if (param->paramkind == PARAM_EXTERN)
            return true;  // Found external parameter, stop search
        return false;
    }

    // Handle subqueries recursively
    if (IsA(node, Query)) {
        return query_tree_walker((Query *) node, query_contains_extern_params_walker, context, 0);
    }

    // Continue traversal for other node types
    return expression_tree_walker(node, query_contains_extern_params_walker, context);
}
```