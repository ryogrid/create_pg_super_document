# calcstrlen

## Location
[src/backend/utils/adt/tsquery_cleanup.c:363-386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_cleanup.c#L363-L386)

## Overview
Recursively calculates the total string length needed to represent all operand values in a TSQuery tree structure.

## Definition

```c
static int32
calcstrlen(NODE *node)
```
## Detailed Description
The `calcstrlen` function traverses a TSQuery tree and computes the total length of string data required to store all the operand values (search terms) contained within the tree. This calculation is essential for memory allocation when converting the tree structure back to a flat TSQuery representation.

For value nodes (QI_VAL), the function returns the length of the stored operand plus one (likely for null termination or delimiter). For operator nodes (QI_OPR), it recursively sums the string lengths from child nodes, with special handling for NOT operators which only have a right child.

The function is typically used during TSQuery cleanup operations to determine how much memory needs to be allocated for the cleaned query structure.

## Parameters / Member Variables
- `node`: Root node of the tree/subtree for which to calculate string length

## Dependencies
- Functions called/Symbols referenced:
  - [calcstrlen](calcstrlen.md): Recursive self-call for child nodes
  - `QI_VAL`: Query item type constant for value nodes
  - `QI_OPR`: Query item type constant for operator nodes
  - `OP_NOT`: Operator type constant for NOT operations
  - `NODE`: Tree node structure type

- Called from (representative examples):
  - [cleanup_tsquery_stopwords](cleanup_tsquery_stopwords.md): Uses the calculated length for memory allocation in tsquery_cleanup.c:420
  - Self-recursive calls for tree traversal

## Notes and Other Information
- This is a static (internal) helper function within the TSQuery cleanup module
- The function assumes a properly formed TSQuery tree structure
- NOT operators are handled specially since they have only a right child (unary operator)
- The calculated length includes space for operand text plus additional overhead (likely delimiters/terminators)
- Used primarily for memory management during TSQuery processing and cleanup operations
- The function performs a simple tree traversal without stack depth checking since string length calculation is lightweight