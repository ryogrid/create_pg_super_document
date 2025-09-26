# QTNClearFlags

## Location
[src/backend/utils/adt/tsquery_util.c:434-448](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_util.c#L434-L448)

## Overview
Recursively clears specified flag bits from all nodes in a QTNode tree structure.

## Definition
```c
void QTNClearFlags(QTNode *in, uint32 flags)
```

## Detailed Description
QTNClearFlags traverses a QTNode tree recursively and clears the specified flag bits from each node's flags field using bitwise AND with the complement of the flags parameter. The function only recurses into child nodes for non-leaf nodes (those not of type QI_VAL), optimizing traversal by stopping at leaf nodes. Stack depth checking prevents potential stack overflow during deep recursion.

## Parameters / Member Variables
- `in`: QTNode tree structure where flags should be cleared
- `flags`: uint32 bitmask specifying which flag bits to clear

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (prevents stack overflow during recursion)
  - QI_VAL (query item type constant for leaf nodes)
  - [QTNClearFlags](QTNClearFlags.md) (recursive self-call for child nodes)
- Called from (representative examples):
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md) (query rewriting operations)
  - [QTNClearFlags](QTNClearFlags.md) (recursive self-calls)

## Notes and Other Information
- Uses bitwise AND with complement (~flags) to clear specific bits while preserving others
- Only traverses internal nodes, stopping recursion at QI_VAL leaf nodes for efficiency
- Essential for cleaning up temporary flags used during query processing operations
- Stack depth checking prevents issues with deeply nested query tree structures
- Commonly used in query rewriting to reset processing state flags