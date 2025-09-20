# disassembledLeaf

## Location
[src/backend/access/gin/gindatapage.c:68-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L68-L102)

## Overview
A structure type used in PostgreSQL's GIN (Generalized Inverted Index) implementation to represent a disassembled leaf page during page modification operations, providing an in-memory representation that facilitates reorganization and recompression.

## Definition

```c
typedef struct
{
	dlist_node	node;			/* linked list pointers */

	/*-------------
	 * 'action' indicates the status of this in-memory segment, compared to
	 * what's on disk. It is one of the GIN_SEGMENT_* action codes:
	 *
	 * UNMODIFIED	no changes
	 * DELETE		the segment is to be removed. 'seg' and 'items' are
	 *				ignored
	 * INSERT		this is a completely new segment
	 * REPLACE		this replaces an existing segment with new content
	 * ADDITEMS		like REPLACE, but no items have been removed, and we track
	 *				in detail what items have been added to this segment, in
	 *				'modifieditems'
	 *-------------
	 */
	char		action;

	ItemPointerData *modifieditems;
	uint16		nmodifieditems;

	/*
	 * The following fields represent the items in this segment. If 'items' is
	 * not NULL, it contains a palloc'd array of the items in this segment. If
	 * 'seg' is not NULL, it contains the items in an already-compressed
	 * format. It can point to an on-disk page (!modified), or a palloc'd
	 * segment in memory. If both are set, they must represent the same items.
	 */
	GinPostingList *seg;
	ItemPointer items;
	int			nitems;			/* # of items in 'items', if items != NULL */
} leafSegmentInfo;
```
## Detailed Description
The  structure is a key component of PostgreSQL's GIN index leaf page management system. It provides an in-memory representation of a leaf page that has been broken down into manageable segments for modification operations such as insertions, deletions, and page splits. This structure enables efficient reorganization of posting list data within leaf pages while maintaining the compressed format used by GIN indexes.

The structure supports both legacy (pre-9.4) and current page formats, and includes provisions for Write-Ahead Logging (WAL) data generation when page modifications need to be logged for crash recovery purposes.

## Parameters / Member Variables
- `node`: A doubly-linked list head containing leafSegmentInfo structures that represent the individual segments of the disassembled page
- `action`: Pointer to the last segment that should remain on the left page during a page split operation
- `*modifieditems`: Total size in bytes of all segments that will be placed on the left page after a split
- `nmodifieditems`: Total size in bytes of all segments that will be placed on the right page after a split
- `*seg`: Boolean flag indicating whether the original page was stored in the pre-9.4 format on disk
- `items`: Buffer containing WAL (Write-Ahead Log) data representing the reconstructed leaf page
- `nitems`: Length of the WAL data buffer in bytes
## Dependencies
- Functions called/Symbols referenced:
  - [dlist_node](dlist_node.md)
  - [GinPostingList](../G/GinPostingList.md)

- Called from (representative examples):
  - [dataBeginPlaceToPageLeaf](dataBeginPlaceToPageLeaf.md)
  - [dataExecPlaceToPageLeaf](dataExecPlaceToPageLeaf.md)
  - [ginVacuumPostingTreeLeaf](../g/ginVacuumPostingTreeLeaf.md)
  - [computeLeafRecompressWALData](../c/computeLeafRecompressWALData.md)
  - [dataPlaceToPageLeafRecompress](dataPlaceToPageLeafRecompress.md)
  - [dataPlaceToPageLeafSplit](dataPlaceToPageLeafSplit.md)
  - [disassembleLeaf](disassembleLeaf.md)
  - [addItemsToLeaf](../a/addItemsToLeaf.md)
  - [leafRepackItems](../l/leafRepackItems.md)

## Notes and Other Information
- This structure is primarily used during GIN index maintenance operations where leaf pages need to be modified or split
- The segment-based approach allows for efficient handling of compressed posting lists without requiring full decompression of unchanged segments
- The split-related fields (lastleft, lsize, rsize) are populated by the leafRepackItems function when determining how to distribute segments across pages during a split
- WAL data generation is conditional and only performed when needed for crash recovery logging
- The structure supports backward compatibility with pre-PostgreSQL 9.4 page formats through the oldformat flag