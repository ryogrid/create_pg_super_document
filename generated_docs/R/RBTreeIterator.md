# RBTreeIterator

## Location
[src/include/lib/rbtree.h:46-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/rbtree.h#L46-L48)

## Overview
RBTreeIterator is a state-holding structure that enables traversal of red-black trees in PostgreSQL, supporting both left-to-right and right-to-left iteration patterns.

## Definition

```c
typedef struct RBTreeIterator RBTreeIterator;
```
## Detailed Description
RBTreeIterator provides a stateful mechanism for traversing red-black trees in PostgreSQL. It encapsulates the current position and direction of tree traversal, allowing for efficient in-order traversal without requiring recursive function calls or explicit stack management. The iterator supports both left-to-right (ascending) and right-to-left (descending) traversal modes through function pointers. The structure is designed to be stack-allocatable for performance while maintaining encapsulation through an opaque interface pattern.

## Parameters / Member Variables
- : Pointer to the RBTree being iterated over, providing access to the tree structure and metadata
- : Function pointer to the iteration strategy (left-to-right or right-to-left), determining traversal direction
- : Pointer to the most recently visited node, used to maintain position during iteration
- : Boolean flag indicating whether the iteration has completed and no more nodes remain

## Dependencies
- Functions called/Symbols referenced:
  - [RBTree](RBTree.md) (tree structure reference)
  - [RBTNode](RBTNode.md) (node type for traversal)
- Called from (representative examples):
  - [rbt_left_right_iterator](../r/rbt_left_right_iterator.md) (ascending traversal setup)
  - [rbt_right_left_iterator](../r/rbt_right_left_iterator.md) (descending traversal setup)  
  - [rbt_begin_iterate](../r/rbt_begin_iterate.md) (iterator initialization)
  - [rbt_iterate](../r/rbt_iterate.md) (iteration step execution)
  - testleftright (test module usage)
  - testrightleft (test module usage)

## Notes and Other Information
- [RBTreeIterator](RBTreeIterator.md) must be treated as an opaque structure by callers despite being declared in the header for stack allocation
- Supports efficient in-order traversal without recursion or explicit stack management
- The function pointer design allows the same iterator structure to support multiple traversal strategies
- Iterator state is maintained across calls, enabling pause-and-resume iteration patterns
- Designed for stack allocation to avoid memory management overhead during tree traversal
- The is_over flag prevents continued iteration attempts after the tree has been fully traversed