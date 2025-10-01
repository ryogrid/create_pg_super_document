# index_expression_changed_walker

## Location
[src/backend/executor/execIndexing.c:1077-1099](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execIndexing.c#L1077-L1099)

## Overview
A static recursive helper function that traverses index expressions to detect if any variables reference columns that have been updated, supporting index optimization decisions.

## Definition

```c
static bool
index_expression_changed_walker(Node *node, Bitmapset *allUpdatedCols)
```
## Detailed Description
This function implements a specialized tree walker designed to analyze index expressions and determine if they reference any columns that have been updated in an UPDATE operation. It follows PostgreSQL's expression tree walker pattern, recursively traversing the abstract syntax tree of index expressions.

The function operates by examining each node in the expression tree. When it encounters a  node (representing a column reference), it checks if that column's attribute number appears in the set of updated columns. If a match is found, it immediately returns true, indicating that the index expression has changed and optimization hints should not be used.

For non-Var nodes, the function delegates to the standard  mechanism, which handles the recursive traversal of the expression tree structure while maintaining the same analysis context.

## Parameters / Member Variables
- : The current node in the expression tree being examined (can be NULL)
- : Bitmapset containing the attribute numbers of all columns that have been updated

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_member](../b/bms_is_member.md)
  - FirstLowInvalidHeapAttributeNumber
  - expression_tree_walker
  - [index_expression_changed_walker](index_expression_changed_walker.md) (recursive call)
- Called from (representative examples):
  - [index_unchanged_by_update](index_unchanged_by_update.md)
  - [index_expression_changed_walker](index_expression_changed_walker.md) (recursive calls)

## Notes and Other Information
- This is a static function used exclusively within execIndexing.c as a helper for index optimization analysis
- Implements the standard PostgreSQL expression tree walker pattern for safe tree traversal
- Uses recursive self-calls through expression_tree_walker to handle complex nested expressions
- The function performs short-circuit evaluation - it returns true immediately upon finding the first variable that references an updated column
- Attribute number adjustment using FirstLowInvalidHeapAttributeNumber accounts for PostgreSQL's internal attribute numbering scheme
- Essential for determining whether indexes with expressions can benefit from the 'indexUnchanged' optimization hint during UPDATE operations

## Simplified Source

```c
static bool
index_expression_changed_walker(Node *node, Bitmapset *allUpdatedCols)
{
    if (node == NULL)
        return false;

    // Check if this is a variable reference
    if (IsA(node, Var)) {
        Var *var = (Var *) node;

        // Check if this variable's column was updated
        if (bms_is_member(var->varattno - FirstLowInvalidHeapAttributeNumber,
                         allUpdatedCols)) {
            // Found an updated column - expression has changed
            return true;
        }

        // This variable wasn't updated
        return false;
    }

    // For other node types, recursively check child nodes
    return expression_tree_walker(node, index_expression_changed_walker,
                                 (void *) allUpdatedCols);
}
```