# spgxlogPickSplit

## Location
[src/include/access/spgxlog.h:165-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgxlog.h#L165-L197)

## Overview
The spgxlogPickSplit struct is a PostgreSQL WAL (Write-Ahead Logging) record structure used to log pick-split operations in SP-GiST (Space-Partitioned Generalized Search Tree) indexes, which occurs when a leaf page becomes full and needs to be split.

## Definition

```c
typedef struct spgxlogPickSplit
{
	bool		isRootSplit;

	uint16		nDelete;		/* n to delete from Src */
	uint16		nInsert;		/* n to insert on Src and/or Dest */
	bool		initSrc;		/* re-init the Src page? */
	bool		initDest;		/* re-init the Dest page? */

	/* where to put new inner tuple */
	OffsetNumber offnumInner;
	bool		initInner;		/* re-init the Inner page? */

	bool		storesNulls;	/* pages are in the nulls tree? */

	/* where the parent downlink is, if any */
	bool		innerIsParent;	/* is parent the same as inner page? */
	OffsetNumber offnumParent;
	uint16		nodeI;

	spgxlogState stateSrc;

	/*----------
	 * data follows:
	 *		array of deleted tuple numbers, length nDelete
	 *		array of inserted tuple numbers, length nInsert
	 *		array of page selector bytes for inserted tuples, length nInsert
	 *		new inner tuple (unaligned!)
	 *		list of leaf tuples, length nInsert (unaligned!)
	 *----------
	 */
	OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
} spgxlogPickSplit;
```
## Detailed Description
This structure represents a WAL record for SP-GiST pick-split operations, one of the most complex operations in SP-GiST index maintenance. A pick-split occurs when a leaf page becomes full and needs to be reorganized by creating a new inner node and potentially redistributing tuples between the original page and a new destination page. The struct contains all the necessary information to redo this operation during WAL replay, including which tuples to delete and insert, page initialization flags, and the new inner tuple structure.

## Parameters / Member Variables
- `isRootSplit`: Indicates whether this is a root page split operation
- `nDelete`: Number of tuples to delete from the source page
- `nInsert`: Number of tuples to insert on source and/or destination pages
- `initSrc`: Flag indicating whether to re-initialize the source page
- `initDest`: Flag indicating whether to re-initialize the destination page
- `offnumInner`: Offset number where the new inner tuple should be placed
- `initInner`: Flag indicating whether to re-initialize the inner page
- `storesNulls`: Flag indicating whether the pages are in the nulls tree portion of the index
- `innerIsParent`: Flag indicating whether the parent page is the same as the inner page
- `offnumParent`: Offset number for the parent downlink location
- `nodeI`: Node index for the parent relationship
- `stateSrc`: SP-GiST state information containing transaction ID and build flag
- `offsets[FLEXIBLE_ARRAY_MEMBER]`: Flexible array member containing variable-length data including deleted/inserted tuple numbers, page selectors, new inner tuple, and leaf tuples
## Dependencies
- Functions called/Symbols referenced:
  - [spgxlogState](spgxlogState.md)
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [doPickSplit](../d/doPickSplit.md) (src/backend/access/spgist/spgdoinsert.c:709)
  - [spgRedoPickSplit](spgRedoPickSplit.md) (src/backend/access/spgist/spgxlog.c:533)
  - [spg_desc](spg_desc.md) (src/backend/access/rmgrdesc/spgdesc.c:85)
  - SizeOfSpgxlogPickSplit (src/include/access/spgxlog.h:199)

## Notes and Other Information
- The structure uses a flexible array member to store variable-length data at the end
- Buffer references in the associated rdata array follow a specific pattern: Backup Blk 0 (Src page, only if not root), Backup Blk 1 (Dest page if used), Backup Blk 2 (Inner page), Backup Blk 3 (Parent page if different from Inner)
- The variable data section contains multiple arrays and structures that are unaligned, requiring careful handling during serialization/deserialization
- This is part of the SP-GiST WAL logging system that ensures crash recovery and replication consistency for space-partitioned index operations