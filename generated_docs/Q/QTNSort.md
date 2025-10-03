# QTNSort

## Location
[src/backend/utils/adt/tsquery_util.c:163-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_util.c#L163-L182)

## Overview
Canonicalizes a QTNode tree by recursively sorting the children of AND/OR operator nodes into a well-defined order, excluding phrase operators which must maintain operand order.

## Definition

```c
void
QTNSort(QTNode *in)
```
## Detailed Description
QTNSort performs recursive canonicalization of a QTNode tree by sorting the children of operator nodes. This canonicalization is crucial for query normalization, enabling consistent representation of logically equivalent queries and supporting efficient query comparison and optimization operations.

The function operates in two phases:
1. **Recursive descent**: Recursively sorts all subtrees before processing the current node
2. **Local sorting**: Sorts immediate children of AND/OR operators using qsort with the cmpQTN comparator

The sorting is applied only to commutative operators (AND, OR) where operand order doesn't affect semantics. Phrase operators (OP_PHRASE) are specifically excluded because they require positional relationships to be preserved between operands.

## Parameters / Member Variables
- `*in`: Pointer to the root QTNode of the tree/subtree to canonicalize
## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - [QTNSort](QTNSort.md) (recursive self-call)
  - qsort (standard library sorting function)
  - [cmpQTN](../c/cmpQTN.md) (QTNode pointer comparison function)
- Data types and constants used:
  - [QTNode](QTNode.md)
  - QI_OPR (operator query item type)
  - OP_PHRASE (phrase operator constant)
- Called from (representative examples):
  - [findeq](../f/findeq.md)
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md)
  - [tsquery_rewrite](../t/tsquery_rewrite.md)

## Notes and Other Information
- Only processes operator nodes (QI_OPR); returns immediately for value nodes
- Preserves phrase operator child ordering since positional semantics matter
- Uses post-order traversal to ensure children are canonicalized before parents
- Stack depth checking prevents overflow during deep recursion
- Essential for query tree normalization and equivalence testing
- Enables consistent internal representation regardless of original query structure
- Critical component of the tsquery rewrite and optimization system