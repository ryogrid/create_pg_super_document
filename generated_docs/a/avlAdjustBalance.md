# avlAdjustBalance

## Location
[src/bin/psql/crosstabview.c:506-528](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/crosstabview.c#L506-L528)

## Overview
Rebalances an AVL tree node after insertion to ensure height differences between left and right subtrees don't exceed 1, maintaining AVL tree properties.

## Definition
static void avlAdjustBalance(avl_tree *tree, avl_node **node)

## Detailed Description
The avlAdjustBalance function is responsible for maintaining the balanced property of an AVL tree after node insertions. It checks the balance factor of a node and performs the necessary rotations to restore balance when the height difference between left and right subtrees exceeds the allowed range (-1, 0, 1). The function implements the standard AVL rebalancing algorithm, which may involve single or double rotations depending on the balance factors of the node and its children. After rebalancing, it updates the height of the affected node to maintain accurate height information throughout the tree.

## Parameters / Member Variables
- tree: Pointer to the AVL tree structure containing metadata about the tree
- node: Double pointer to the node being rebalanced; may be modified to point to the new root of the subtree after rotation

## Dependencies
- Functions called/Symbols referenced:
  - [avlBalance](avlBalance.md)
  - [avlRotate](avlRotate.md)
  - [avlUpdateHeight](avlUpdateHeight.md)
  - avl_tree
  - avl_node
- Called from (representative examples):
  - [avlInsertNode](avlInsertNode.md)

## Notes and Other Information
This function implements the core AVL tree balancing logic used in PostgreSQL's crosstab view functionality. It uses integer arithmetic to determine rotation directions efficiently, where the balance factor is divided by 2 to determine if rebalancing is needed (b != 0), and the direction is calculated using (1-b)/2. The function handles both single and double rotation cases by first checking if a double rotation is needed (when child balance factor equals -b) before performing the main rotation. The function only updates height for nodes that are not the tree end marker, ensuring proper tree maintenance.

## Simplified Source

```c
static void avlAdjustBalance(avl_tree *tree, avl_node **node) {
    avl_node *current = *node;
    int balance = avlBalance(current) / 2;

    // Check if rebalancing is needed
    if (balance != 0) {
        int direction = (1 - balance) / 2;

        // Double rotation needed?
        if (avlBalance(current->children[direction]) == -balance) {
            avlRotate(&current->children[direction], !direction);
        }

        // Perform main rotation
        current = avlRotate(node, direction);
    }

    // Update height if not at tree end
    if (current != tree->end) {
        avlUpdateHeight(current);
    }
}
```