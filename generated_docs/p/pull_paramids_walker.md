# pull_paramids_walker

## Location
[src/backend/optimizer/util/clauses.c:5428-5441](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L5428-L5441)

## Overview
A static walker function that recursively traverses an expression tree to collect parameter IDs from Param nodes and accumulates them into a Bitmapset.

## Definition
```c
static bool pull_paramids_walker(Node *node, Bitmapset **context)
```

## Detailed Description
The `pull_paramids_walker` function is the core implementation of the parameter ID extraction logic in PostgreSQL. It uses the expression tree walker pattern to recursively traverse nodes in an expression tree, specifically looking for Param nodes. When a Param node is encountered, it extracts the parameter ID (paramid) and adds it to the Bitmapset pointed to by the context parameter.

This function follows the standard walker function pattern in PostgreSQL, returning false to continue traversal and using `expression_tree_walker` to handle the recursive traversal of child nodes. The function is designed to be used with the expression tree walker infrastructure, which provides a systematic way to visit all nodes in an expression tree.

## Parameters / Member Variables
- `node`: A pointer to the current node being examined in the expression tree traversal. Can be NULL.
- `context`: A double pointer to a Bitmapset that accumulates the parameter IDs found during traversal. The function modifies the Bitmapset by adding new parameter IDs to it.

## Dependencies
- Functions called/Symbols referenced:
  - [Param](../P/Param.md) (type check and cast)
  - [bms_add_member](../b/bms_add_member.md)
  - expression_tree_walker
  - [pull_paramids_walker](pull_paramids_walker.md) (recursive call)
- Called from (representative examples):
  - [pull_paramids](pull_paramids.md)
  - max_parallel_hazard_context
  - [pull_paramids_walker](pull_paramids_walker.md) (recursive self-call)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within the same compilation unit (clauses.c)
- Returns false in all cases to continue tree traversal - this is the standard pattern for walker functions that collect information
- Handles NULL nodes gracefully by returning false immediately
- Uses the IsA() macro to check if a node is of type Param
- The function modifies the Bitmapset in-place through the context parameter
- Located in src/backend/optimizer/util/clauses.c at lines 5428-5441
- Follows the expression tree walker pattern used throughout PostgreSQL for systematic tree traversal
- The recursive call through expression_tree_walker ensures all child nodes are properly visited

## Simplified Source

```c
static bool
pull_paramids_walker(Node *node, Bitmapset **context)
{
    // Handle NULL nodes
    if (node == NULL)
        return false;

    // Check if this node is a Param
    if (IsA(node, Param))
    {
        Param *param = (Param *) node;

        // Add the parameter ID to our result set
        *context = bms_add_member(*context, param->paramid);
        return false;
    }

    // Continue traversing the expression tree
    return expression_tree_walker(node, pull_paramids_walker, (void *) context);
}
```