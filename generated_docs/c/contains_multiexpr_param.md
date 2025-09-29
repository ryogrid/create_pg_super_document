# contains_multiexpr_param

## Location
[src/backend/rewrite/rewriteManip.c:325-350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L325-L350)

## Overview
A static helper function that checks whether an expression tree contains any MULTIEXPR Param nodes at the current query level.

## Definition

```c
static bool
contains_multiexpr_param(Node *node, void *context)
```
## Detailed Description
This function performs a tree traversal to detect MULTIEXPR parameters within an expression tree. It's designed to work with PostgreSQL's expression tree walker infrastructure. The function specifically looks for Param nodes with paramkind set to PARAM_MULTIEXPR and intentionally avoids descending into SubLinks, focusing only on parameters at the current query level. When a MULTIEXPR Param is found, it immediately returns true to abort the tree traversal.

## Parameters / Member Variables
- `node`: The current Node being examined in the expression tree traversal
- `context`: Context parameter (not used in this function, but required by expression_tree_walker interface)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - expression_tree_walker (for recursive tree traversal)
  - [Param](../P/Param.md) (structure type)
  - PARAM_MULTIEXPR (parameter kind constant)
- Called from (representative examples):
  - [ReplaceVarsFromTargetList_callback](../R/ReplaceVarsFromTargetList_callback.md)
  - [contains_multiexpr_param](contains_multiexpr_param.md) (recursive self-call via expression_tree_walker)

## Notes and Other Information
- This is a static function within rewriteManip.c, indicating it's an internal utility
- The function follows PostgreSQL's expression tree walker pattern
- Deliberately skips SubLinks to limit scope to current query level
- Used in rewrite rule processing to detect multi-expression parameters
- Returns true immediately upon finding a MULTIEXPR Param to optimize performance

## Simplified Source

```c
static bool contains_multiexpr_param(Node *node, void *context) {
    if (node == NULL)
        return false;

    // Check if this is a MULTIEXPR parameter
    if (IsA(node, Param)) {
        if (((Param *) node)->paramkind == PARAM_MULTIEXPR)
            return true; // Found one, abort traversal
        return false;
    }

    // Recursively check child nodes
    return expression_tree_walker(node, contains_multiexpr_param, context);
}
```