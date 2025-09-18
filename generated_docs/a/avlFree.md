# avlFree

## Location
src/bin/psql/crosstabview.c: 448 - 471

## Overview
Recursively deallocates memory for an AVL tree structure, freeing all nodes and the sentinel end node.

## Definition
```c
static void avlFree(avl_tree *tree, avl_node *node)
```

## Detailed Description
avlFree is a cleanup function that performs recursive deallocation of an AVL tree structure. It traverses the tree in a post-order fashion (children first, then parent) to ensure safe memory deallocation without accessing freed memory.

The function uses the sentinel end node as a termination condition for recursion. When it encounters a child that points to the tree's end sentinel, it knows it has reached a leaf position and stops recursing. The function handles the special case of the root node by freeing it separately since it's not a child of any other node.

The sentinel end node (tree->end) is freed only once, when the root node is being processed, ensuring that the tree structure is completely cleaned up after the operation.

## Parameters / Member Variables
- `tree`: Pointer to the avl_tree structure containing the sentinel end node reference
- `node`: Current node being processed for deallocation (starting from root)

## Dependencies
- Functions called/Symbols referenced:
  - pg_free (for memory deallocation)
  - avlFree (recursive self-calls for tree traversal)
  - avl_tree, avl_node (structure types)
- Called from (representative examples):
  - PrintResultInCrosstab (src/bin/psql/crosstabview.c:273, 274)
  - avlFree (recursive calls at src/bin/psql/crosstabview.c:452, 457)

## Notes and Other Information
- Uses post-order traversal to ensure children are freed before their parent
- Sentinel end node (tree->end) serves as recursion termination condition
- Root node is handled specially since it has no parent
- The sentinel end node is freed only once during root processing
- Recursive implementation ensures complete tree cleanup
- Safe memory management prevents access to freed memory during traversal
- Part of the minimalistic AVL tree implementation used for crosstab header collection