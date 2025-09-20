# gistxlogDelete

## Location
[src/include/access/gistxlog.h:50-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gistxlog.h#L50-L59)

## Overview
The  structure represents a WAL (Write-Ahead Logging) record for GiST index tuple deletion operations, capturing information needed to replay deletion operations during recovery.

## Definition

```c
typedef struct gistxlogDelete
{
	TransactionId snapshotConflictHorizon;
	uint16		ntodelete;		/* number of deleted offsets */
	bool		isCatalogRel;	/* to handle recovery conflict during logical
								 * decoding on standby */

	/* TODELETE OFFSET NUMBERS */
	OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
} gistxlogDelete;
```
## Detailed Description
This structure is used to log GiST index tuple deletion operations in the write-ahead log. It contains all the information necessary to replay the deletion during crash recovery or streaming replication. The structure includes a snapshot conflict horizon for handling recovery conflicts, the number of tuples to delete, a flag for catalog relations, and a flexible array of offset numbers identifying which tuples to delete from the target page.

## Parameters / Member Variables
- `snapshotConflictHorizon`: Transaction ID used to determine snapshot conflicts during recovery, ensuring proper MVCC visibility semantics
- `ntodelete`: Number of index tuples being deleted in this operation
- `isCatalogRel`: Boolean flag indicating if this is a catalog relation, used for handling recovery conflicts during logical decoding on standby servers
- `offsets[FLEXIBLE_ARRAY_MEMBER]`: Flexible array containing the offset numbers of tuples to be deleted from the leaf page
## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [gistRedoDeleteRecord](gistRedoDeleteRecord.md)
  - [gistXLogDelete](gistXLogDelete.md)
  - [out_gistxlogDelete](../o/out_gistxlogDelete.md)
  - [gist_desc](gist_desc.md)
  - SizeOfGistxlogDelete

## Notes and Other Information
- This structure is used specifically for leaf pages in GiST indexes where index tuples are deleted
- The backup block 0 contains the leaf page whose index tuples are being deleted
- The flexible array member allows for variable-length records depending on the number of tuples being deleted
- Recovery conflict handling is particularly important for logical decoding scenarios on standby servers