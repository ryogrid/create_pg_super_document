# fsm_rebuild_page

## Location
[src/backend/storage/freespace/fsmpage.c:342-374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/fsmpage.c#L342-L374)

## Overview
Reconstructs the upper levels of a Free Space Map (FSM) page by recalculating non-leaf node values based on their children to maintain the FSM tree structure integrity.

## Definition

```c
bool
fsm_rebuild_page(Page page)
```
## Detailed Description
The  function is responsible for maintaining the consistency of the FSM tree structure after modifications to leaf nodes. FSM pages are organized as binary trees where leaf nodes represent the available space in individual heap pages, and non-leaf nodes store the maximum available space among their children.

The function works by traversing all non-leaf nodes from the bottom level upwards, recalculating each node's value as the maximum of its two children. It starts from the lowest non-leaf level and works backwards through all levels up to the root, ensuring that parent nodes correctly reflect the maximum available space in their subtrees.

This rebuilding process is essential after operations that modify leaf nodes (such as  or ) to ensure that the FSM can efficiently locate pages with sufficient free space for new tuple insertions.

## Parameters / Member Variables
- : The FSM page whose upper levels need to be reconstructed, represented as a generic Page structure

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetContents](../P/PageGetContents.md): Extracts the FSM page content from the generic page structure
  - leftchild: Macro/function to calculate the left child node index for a given parent node
- Constants referenced:
  - FSMPage: Type definition for FSM page structure
  - NonLeafNodesPerPage: Number of non-leaf nodes per FSM page
  - NodesPerPage: Total number of nodes per FSM page
- Called from (representative examples):
  - [fsm_set_avail](fsm_set_avail.md): After setting available space for a specific slot
  - [fsm_search_avail](fsm_search_avail.md): After searching and potentially modifying available space
  - [fsm_truncate_avail](fsm_truncate_avail.md): After truncating slots in an FSM page

## Notes and Other Information
- Returns  if the page was modified during reconstruction,  if no changes were needed
- The algorithm processes nodes in reverse order (from highest index to lowest) within each level
- For each non-leaf node, it calculates the new value as the maximum of its left and right children
- Some nodes at the boundary may have zero or one child, which is handled correctly by the logic
- The function only updates a node's value if it differs from the calculated value, minimizing unnecessary modifications
- This function is critical for maintaining FSM performance, as incorrect upper-level values would lead to inefficient space allocation decisions
- The fp_nodes array represents the complete binary tree structure, with leaf nodes at the end and non-leaf nodes at the beginning

## Simplified Source

```c
bool
fsm_rebuild_page(Page page)
{
    FSMPage fsmpage = (FSMPage) PageGetContents(page);
    bool changed = false;

    // Traverse non-leaf nodes from bottom to top, updating parent values
    for (int nodeno = NonLeafNodesPerPage - 1; nodeno >= 0; nodeno--)
    {
        int lchild = leftchild(nodeno);
        int rchild = lchild + 1;
        uint8 newvalue = 0;

        // Calculate maximum value of children
        if (lchild < NodesPerPage)
            newvalue = fsmpage->fp_nodes[lchild];
        if (rchild < NodesPerPage)
            newvalue = Max(newvalue, fsmpage->fp_nodes[rchild]);

        // Update node if value changed
        if (fsmpage->fp_nodes[nodeno] != newvalue)
        {
            fsmpage->fp_nodes[nodeno] = newvalue;
            changed = true;
        }
    }

    return changed;
}
```