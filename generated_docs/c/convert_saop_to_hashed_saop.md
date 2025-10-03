# convert_saop_to_hashed_saop

## Location
[src/backend/optimizer/util/clauses.c:2287-2292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L2287-L2292)

## Overview
This function optimizes ScalarArrayOpExpr nodes by identifying opportunities to use hash table evaluation instead of linear search for array operations, improving performance for large arrays with constant elements.

## Definition

```c
void
convert_saop_to_hashed_saop(Node *node)
```
## Detailed Description
The  function performs a recursive optimization pass on expression trees to identify ScalarArrayOpExpr nodes that would benefit from hash table evaluation. It analyzes array operations and fills in hash function information when certain conditions are met, allowing the executor to use more efficient hash-based lookups instead of linear searches through array elements.

The optimization applies when all these conditions are satisfied:
1. The array (2nd argument) contains only constant values
2. Either useOr is true OR there's a valid negator operator for the operation
3. Valid hash functions exist for both operands and they are compatible
4. The array is large enough to justify the hash table overhead

This optimization is particularly beneficial for expressions like "column IN (const1, const2, ..., constN)" where N is large.

## Parameters / Member Variables
- `*node`: Node pointer to the expression tree to be analyzed and potentially optimized for hash-based array operations
## Dependencies
- Functions called/Symbols referenced:
  - [convert_saop_to_hashed_saop_walker](convert_saop_to_hashed_saop_walker.md)
- Called from (representative examples):
  - [preprocess_expression](../p/preprocess_expression.md) (planner.c:1224)

## Notes and Other Information
- Part of the query optimization pipeline, typically called during expression preprocessing
- The actual work is delegated to convert_saop_to_hashed_saop_walker which performs the recursive tree traversal
- This optimization can significantly improve performance for large IN clauses and similar array operations
- The hash table approach trades setup cost for faster lookups when array size justifies it

## Simplified Source

```c
void
convert_saop_to_hashed_saop(Node *node)
{
    // Recursively search expression tree for ScalarArrayOpExprs
    // and optimize them for hash table evaluation when beneficial
    (void) convert_saop_to_hashed_saop_walker(node, NULL);
}
```