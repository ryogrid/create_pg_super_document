# SPPageDesc

## Location
[src/backend/access/spgist/spgdoinsert.c:36-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L36-L43)

## Overview
SPPageDesc is a structure that tracks all information about a page during SP-GiST index insertion operations, providing a unified descriptor that can identify pages, tuples, or specific nodes within inner tuples.

## Definition

```c
typedef struct SPPageDesc
{
	BlockNumber blkno;			/* block number, or InvalidBlockNumber */
	Buffer		buffer;			/* page's buffer number, or InvalidBuffer */
	Page		page;			/* pointer to page buffer, or NULL */
	OffsetNumber offnum;		/* offset of tuple, or InvalidOffsetNumber */
	int			node;			/* node number within inner tuple, or -1 */
} SPPageDesc;
```
## Detailed Description
SPPageDesc serves as a comprehensive descriptor for tracking pages during SP-GiST insertion operations. The structure is designed with flexibility in mind - any of its fields can be invalid depending on the context. The key invariant is that if the buffer field is valid, it implies the caller holds both a pin and exclusive lock on that buffer. Additionally, the page pointer should be valid exactly when the buffer is valid, maintaining consistency between these related fields.

This structure can represent different levels of specificity:
- A block number reference (when only blkno is valid)
- A buffered page reference (when buffer and page are valid)  
- A specific tuple within a page (when offnum is also valid)
- A specific node within an inner tuple (when node is also valid)

## Parameters / Member Variables
- `blkno`: Block number identifying the physical page, or InvalidBlockNumber if not applicable
- `buffer`: Buffer number for the page in the buffer pool, or InvalidBuffer if not buffered
- `page`: Direct pointer to the page buffer in memory, or NULL if not available
- `offnum`: Offset number identifying a specific tuple within the page, or InvalidOffsetNumber if not targeting a specific tuple
- `node`: Node number within an inner tuple for fine-grained positioning, or -1 if not applicable
## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a data structure definition)
- Called from (representative examples):
  - [saveNodeLink](../s/saveNodeLink.md)
  - [addLeafTuple](../a/addLeafTuple.md)
  - [checkSplitConditions](../c/checkSplitConditions.md)
  - [moveLeafs](../m/moveLeafs.md)
  - [setRedirectionTuple](../s/setRedirectionTuple.md)
  - [doPickSplit](../d/doPickSplit.md)
  - [spgMatchNodeAction](../s/spgMatchNodeAction.md)
  - [spgAddNodeAction](../s/spgAddNodeAction.md)
  - [spgSplitNodeAction](../s/spgSplitNodeAction.md)
  - [spgdoinsert](../s/spgdoinsert.md)

## Notes and Other Information
- This structure is central to SP-GiST insertion logic, used throughout the spgdoinsert.c module
- The design allows for progressive refinement of page/tuple/node identification during insertion traversal
- Buffer management invariants must be carefully maintained when using this structure
- The flexible validity of fields makes this structure suitable for different phases of the insertion algorithm
- Located in src/backend/access/spgist/spgdoinsert.c:36-43