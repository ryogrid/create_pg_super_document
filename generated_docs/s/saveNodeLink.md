# saveNodeLink

## Location
[src/backend/access/spgist/spgdoinsert.c:186-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L186-L202)

## Overview
Updates a parent inner tuple's downlink to point to a new location and marks the parent buffer as dirty, typically used as the final step in SP-GiST tree modification operations.

## Definition

```c
static void
saveNodeLink(Relation index, SPPageDesc *parent,
			 BlockNumber blkno, OffsetNumber offnum)
```
## Detailed Description
This function performs the critical final step in many SP-GiST tree modification operations by updating a parent inner tuple's downlink to point to a new block and offset location. It retrieves the inner tuple from the parent page using the page descriptor information, calls spgUpdateNodeLink to modify the appropriate node's item pointer, and then marks the parent buffer as dirty to ensure the change is persisted. The function is designed to be the last modification made to a parent page during a WAL action, ensuring proper write-ahead logging semantics.

## Parameters / Member Variables
- `index`: The SP-GiST relation being modified
- `parent`: Page descriptor containing information about the parent page (buffer, page, offnum, node)
- `blkno`: The new block number that the downlink should point to
- `offnum`: The new offset number within the block that the downlink should point to

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetItem](../P/PageGetItem.md) (retrieves tuple from page)
  - [PageGetItemId](../P/PageGetItemId.md) (gets item identifier for tuple)
  - [spgUpdateNodeLink](spgUpdateNodeLink.md) (updates the specific node's downlink)
  - [MarkBufferDirty](../M/MarkBufferDirty.md) (marks buffer for write-ahead logging)
- Called from (representative examples):
  - [addLeafTuple](../a/addLeafTuple.md)
  - [moveLeafs](../m/moveLeafs.md)
  - [doPickSplit](../d/doPickSplit.md)
  - [spgAddNodeAction](spgAddNodeAction.md)

## Notes and Other Information
- The function is static, meaning it's only accessible within the spgdoinsert.c module
- Designed to be the final modification to a parent page in a WAL action
- The parent page descriptor (SPPageDesc) contains all necessary information: buffer, page, offset number, and node index
- Critical for maintaining SP-GiST tree integrity after node splits, moves, or additions
- Marking the buffer dirty is essential for proper WAL logging and crash recovery
- Located in src/backend/access/spgist/spgdoinsert.c:186-202

## Simplified Source

```c
static void saveNodeLink(Relation index, SPPageDesc *parent,
                        BlockNumber blkno, OffsetNumber offnum)
{
    SpGistInnerTuple innerTuple;

    // Get the inner tuple from the parent page
    innerTuple = (SpGistInnerTuple) PageGetItem(parent->page,
                                               PageGetItemId(parent->page, parent->offnum));

    // Update the node's downlink to new location
    spgUpdateNodeLink(innerTuple, parent->node, blkno, offnum);

    // Mark buffer dirty for WAL logging
    MarkBufferDirty(parent->buffer);
}
```