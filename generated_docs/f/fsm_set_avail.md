# fsm_set_avail

## Location
[src/backend/storage/freespace/fsmpage.c:63-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/fsmpage.c#L63-L121)

## Overview
The fsm_set_avail function sets the free space value for a specific slot on a Free Space Map page and propagates changes up the binary tree structure to maintain consistency.

## Definition
```c
bool fsm_set_avail(Page page, int slot, uint8 value)
```

## Detailed Description
This function updates the free space value for a given slot in a Free Space Map page's binary tree structure. It first sets the leaf node value, then propagates the change up through parent nodes to maintain the tree property where each internal node contains the maximum value of its children.

The function performs several key operations:
1. Converts the slot number to the corresponding leaf node position
2. Updates the leaf node with the new value
3. Propagates changes upward through the tree, updating parent nodes with the maximum of their children
4. Includes a sanity check to detect tree corruption and triggers a rebuild if necessary
5. Returns early if the value hasn't actually changed and doesn't exceed the root value

The propagation continues until it reaches the root or encounters a node that doesn't need updating (when the parent's value is already correct).

## Parameters / Member Variables
- `page`: The Free Space Map page to modify (must be exclusively locked by caller)
- `slot`: The slot number to update (must be less than LeafNodesPerPage)  
- `value`: The new free space value (uint8) to set for the slot

## Dependencies
- Functions called/Symbols referenced:
  - `NonLeafNodesPerPage`: Constant defining number of non-leaf nodes
  - `[PageGetContents](../P/PageGetContents.md)`: Extracts page contents as FSMPage structure
  - `FSMPage`: Type representing Free Space Map page data
  - `LeafNodesPerPage`: Constant defining number of leaf nodes per page
  - `parentof`: Macro to calculate parent node index
  - `leftchild`: Macro to calculate left child node index
  - `NodesPerPage`: Total number of nodes per page
  - `[fsm_rebuild_page](fsm_rebuild_page.md)`: Function to rebuild corrupted FSM page
- Called from (representative examples):
  - `[XLogRecordPageWithFreeSpace](../X/XLogRecordPageWithFreeSpace.md)`: Records free space changes in WAL
  - `[fsm_set_and_search](fsm_set_and_search.md)`: Sets value and searches for available space
  - `[fsm_search](fsm_search.md)`: Part of free space search operations
  - `[fsm_vacuum_page](fsm_vacuum_page.md)`: Updates free space during vacuum operations

## Notes and Other Information
- The caller must hold an exclusive lock on the page before calling this function
- Returns `true` if the page was modified, `false` if no change was needed
- Includes optimization to avoid unnecessary work when values haven't changed
- Contains corruption detection logic that triggers page rebuild when tree invariants are violated
- Part of PostgreSQL's Free Space Map system for tracking available space in heap pages
- The binary tree structure ensures efficient searches while maintaining space usage information
- Uses Max() macro to determine the maximum value between left and right children during propagation

## Simplified Source

```c
bool fsm_set_avail(Page page, int slot, uint8 value) {
    int nodeno = NonLeafNodesPerPage + slot;  // Convert slot to leaf node index
    FSMPage fsmpage = (FSMPage) PageGetContents(page);
    uint8 oldvalue;

    Assert(slot < LeafNodesPerPage);

    oldvalue = fsmpage->fp_nodes[nodeno];

    // Early exit if value unchanged and doesn't exceed root
    if (oldvalue == value && value <= fsmpage->fp_nodes[0])
        return false;

    // Set the leaf node value
    fsmpage->fp_nodes[nodeno] = value;

    // Propagate changes up the tree
    do {
        uint8 newvalue = 0;
        int lchild, rchild;

        nodeno = parentof(nodeno);
        lchild = leftchild(nodeno);
        rchild = lchild + 1;

        // Calculate maximum of children
        newvalue = fsmpage->fp_nodes[lchild];
        if (rchild < NodesPerPage)
            newvalue = Max(newvalue, fsmpage->fp_nodes[rchild]);

        oldvalue = fsmpage->fp_nodes[nodeno];
        if (oldvalue == newvalue)
            break;  // No change needed at this level

        fsmpage->fp_nodes[nodeno] = newvalue;
    } while (nodeno > 0);

    // Corruption check: new value shouldn't exceed root value
    if (value > fsmpage->fp_nodes[0])
        fsm_rebuild_page(page);

    return true;
}
```