# spgUpdateNodeLink

## Location
[src/backend/access/spgist/spgdoinsert.c:52-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L52-L79)

## Overview
Updates the item pointer (downlink) in a specific node entry within an SP-GiST inner tuple, typically used after move or split operations to maintain tree structure integrity.

## Definition

```c
void
spgUpdateNodeLink(SpGistInnerTuple tup, int nodeN,
				  BlockNumber blkno, OffsetNumber offset)
```
## Detailed Description
This function modifies the item pointer (t_tid) of the nodeN'th entry in an SP-GiST inner tuple. It's a critical utility function used during tree maintenance operations when nodes are moved or split, requiring parent inner tuples to update their downlinks to point to the new locations of child nodes. The function iterates through the nodes in the inner tuple using the SGITITERATE macro until it finds the target node, then updates its item pointer with the new block number and offset.

## Parameters / Member Variables
- `tup`: The SP-GiST inner tuple containing the nodes to be updated
- `nodeN`: The index (0-based) of the node entry whose downlink needs updating
- `blkno`: The new block number that the downlink should point to
- `offset`: The new offset number within the block that the downlink should point to

## Dependencies
- Functions called/Symbols referenced:
  - SGITITERATE (macro for iterating through nodes in inner tuple)
  - [ItemPointerSet](../I/ItemPointerSet.md) (sets the item pointer with block and offset)
  - elog (for error reporting)
- Called from (representative examples):
  - [saveNodeLink](saveNodeLink.md)
  - [spgSplitNodeAction](spgSplitNodeAction.md)
  - [spgRedoAddLeaf](spgRedoAddLeaf.md)
  - [spgRedoMoveLeafs](spgRedoMoveLeafs.md)
  - [spgRedoAddNode](spgRedoAddNode.md)
  - [spgRedoPickSplit](spgRedoPickSplit.md)

## Notes and Other Information
- The function will throw an ERROR if the requested node index (nodeN) is not found in the inner tuple
- This is a low-level function primarily used during WAL replay and tree restructuring operations
- The function assumes the inner tuple structure is valid and the nodeN parameter is within reasonable bounds
- Located in src/backend/access/spgist/spgdoinsert.c:52-79

## Simplified Source

```c
void spgUpdateNodeLink(SpGistInnerTuple tup, int nodeN,
                       BlockNumber blkno, OffsetNumber offset)
{
    int i;
    SpGistNodeTuple node;

    // Iterate through nodes in the inner tuple
    SGITITERATE(tup, i, node)
    {
        if (i == nodeN)
        {
            // Update the downlink to point to new location
            ItemPointerSet(&node->t_tid, blkno, offset);
            return;
        }
    }

    // Error if node not found
    elog(ERROR, "failed to find requested node %d in SPGiST inner tuple", nodeN);
}
```