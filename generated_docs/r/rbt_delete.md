# rbt_delete

## Location
[src/backend/lib/rbtree.c:695-704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L695-L704)

## Overview
Public API function that removes a specified node from a Red-Black tree, serving as the main entry point for tree deletion operations.

## Definition

```c
void
rbt_delete(RBTree *rbt, RBTNode *node)
```
## Detailed Description
This function provides the public interface for deleting nodes from a Red-Black tree. It serves as a simple wrapper around the internal rbt_delete_node function, providing a clean API for external code to use. The function is designed to be called after a node has been located through other tree operations like rbt_find or rbt_leftmost.

An important design consideration is that this function does not handle the deallocation of any subsidiary data that may be attached to the node being deleted. The caller is explicitly responsible for freeing such data before calling rbt_delete, as the actual physical node that gets freed may be different from the logically deleted node due to the tree successor replacement strategy used in the underlying implementation.

## Parameters / Member Variables
- `*rbt`: Pointer to the Red-Black tree from which the node will be deleted
- `*node`: Pointer to the node to be removed from the tree (must have been previously found via rbt_find or rbt_leftmost)
## Dependencies
- Functions called/Symbols referenced:
  - [rbt_delete_node](rbt_delete_node.md)
  - [RBTree](../R/RBTree.md) (tree structure type)
  - [RBTNode](../R/RBTNode.md) (node structure type)
- Called from (representative examples):
  - [testfindltgt](../t/testfindltgt.md) (test function)
  - [testdelete](../t/testdelete.md) (test function)

## Notes and Other Information
- This is the main public API function for node deletion in Red-Black trees
- The node parameter must have been previously obtained through valid tree operations
- Callers must handle freeing of any subsidiary data attached to nodes before deletion
- The function delegates all actual deletion logic to the internal rbt_delete_node function
- Care must be taken not to rely on the freefunc for subsidiary data cleanup, as a different physical node may be the one actually freed due to tree restructuring during deletion
- Used extensively in PostgreSQL's testing framework to validate Red-Black tree deletion functionality