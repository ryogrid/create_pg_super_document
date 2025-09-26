# QTNEq

## Location
[src/backend/utils/adt/tsquery_util.c:183-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_util.c#L183-L200)

## Overview
QTNEq is a function that determines if two QTNode trees are equal according to the QTNodeCompare function, used in PostgreSQL's text search query processing.

## Definition

```c
bool
QTNEq(QTNode *a, QTNode *b)
```
## Detailed Description
QTNEq performs equality comparison between two QTNode trees by first checking if their signatures are compatible and then using QTNodeCompare for detailed comparison. The function uses a signature-based optimization to quickly eliminate obviously different nodes before performing the more expensive structural comparison.

The function first computes the intersection of the two nodes' signatures and verifies that both nodes have signatures that are subsets of this intersection. Only if this signature check passes does it proceed to call QTNodeCompare for the actual structural comparison.

## Parameters / Member Variables
- : Pointer to the first QTNode tree to compare
- : Pointer to the second QTNode tree to compare

## Dependencies
- Functions called/Symbols referenced:
  - [QTNode](QTNode.md) (structure type)
  - [QTNodeCompare](QTNodeCompare.md) (for detailed node comparison)
- Called from (representative examples):
  - [findeq](../f/findeq.md) (in tsquery_rewrite.c)

## Notes and Other Information
- The function uses signature-based filtering as an optimization to avoid expensive structural comparisons when nodes are clearly different
- Returns true only if both signature compatibility and structural equality (via QTNodeCompare) are satisfied
- Part of PostgreSQL's text search query processing infrastructure
- Located in src/backend/utils/adt/tsquery_util.c:183-200