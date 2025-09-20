# GinBtreeData

## Location
[src/include/access/gin_private.h:150-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gin_private.h#L150-L180)

## Overview
GinBtreeData is a comprehensive structure that provides the operational interface for GIN B-tree manipulation through function pointers and maintains context information for both entry trees and data trees.

## Definition

```c
typedef struct GinBtreeData
{
	/* search methods */
	BlockNumber (*findChildPage) (GinBtree, GinBtreeStack *);
	BlockNumber (*getLeftMostChild) (GinBtree, Page);
	bool		(*isMoveRight) (GinBtree, Page);
	bool		(*findItem) (GinBtree, GinBtreeStack *);

	/* insert methods */
	OffsetNumber (*findChildPtr) (GinBtree, Page, BlockNumber, OffsetNumber);
	GinPlaceToPageRC (*beginPlaceToPage) (GinBtree, Buffer, GinBtreeStack *, void *, BlockNumber, void **, Page *, Page *);
	void		(*execPlaceToPage) (GinBtree, Buffer, GinBtreeStack *, void *, BlockNumber, void *);
	void	   *(*prepareDownlink) (GinBtree, Buffer);
	void		(*fillRoot) (GinBtree, Page, BlockNumber, Page, BlockNumber, Page);

	bool		isData;

	Relation	index;
	BlockNumber rootBlkno;
	GinState   *ginstate;		/* not valid in a data scan */
	bool		fullScan;
	bool		isBuild;

	/* Search key for Entry tree */
	OffsetNumber entryAttnum;
	Datum		entryKey;
	GinNullCategory entryCategory;

	/* Search key for data tree (posting tree) */
	ItemPointerData itemptr;
} GinBtreeData;
```
## Detailed Description
GinBtreeData implements a polymorphic interface for GIN B-tree operations through function pointers, allowing both entry trees and data trees to share common navigation and modification algorithms while providing type-specific implementations. The structure combines operational methods with context information, supporting both search operations and tree modifications. It distinguishes between entry trees (which store keys) and data trees (posting trees, which store item pointers) through the isData flag and provides appropriate search keys for each type.

## Parameters / Member Variables
- `*)`: Function pointer to locate appropriate child page during tree descent
- `Page)`: Function pointer to find the leftmost child page at a given level
- `Page)`: Function pointer to determine if right-link following is needed
- `*)`: Function pointer to locate specific items within a page
- `OffsetNumber)`: Function pointer to find child page pointer within an internal page
- `*)`: Function pointer to initiate page insertion/split operations
- `*)`: Function pointer to execute page modification operations
- `Buffer)`: Function pointer to prepare downlink data for parent updates
- `Page)`: Function pointer to populate a new root page after splits
- `isData`: Boolean flag indicating whether this is a data tree (true) or entry tree (false)
- `index`: Relation object representing the GIN index
- `rootBlkno`: Block number of the tree's root page
- `*ginstate`: Pointer to GinState structure (not used during data scans)
- `fullScan`: Boolean indicating whether a full scan is being performed
- `isBuild`: Boolean indicating whether this is during index build
- `entryAttnum`: Attribute number for entry tree searches
- `entryKey`: Search key value for entry tree operations
- `entryCategory`: Null category for the entry key
- `itemptr`: Item pointer data for data tree searches
## Dependencies
- Functions called/Symbols referenced:
  - [GinBtree](GinBtree.md) (parameter type for function pointers)
  - [GinBtreeStack](GinBtreeStack.md) (parameter type for function pointers)
  - [GinPlaceToPageRC](GinPlaceToPageRC.md) (return type for beginPlaceToPage)
  - [GinState](GinState.md) (for ginstate member)
  - GinNullCategory (for entryCategory member)
- Called from (representative examples):
  - [ginPrepareDataScan](../g/ginPrepareDataScan.md)
  - [ginPrepareEntryScan](../g/ginPrepareEntryScan.md)
  - [ginInsertItemPointers](../g/ginInsertItemPointers.md)
  - [moveRightIfItNeeded](../m/moveRightIfItNeeded.md)
  - [scanPostingTree](../s/scanPostingTree.md)

## Notes and Other Information
- Located in src/include/access/gin_private.h:150-180
- Implements polymorphic behavior for different GIN tree types
- Function pointers are set differently for entry trees vs data trees
- Essential for abstracting differences between tree types while sharing common algorithms
- The ginstate pointer is not valid during data scans, as noted in the comment
- Supports both build-time and runtime operations through the isBuild flag