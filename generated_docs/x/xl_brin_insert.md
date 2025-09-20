# xl_brin_insert

## Location
[src/include/access/brin_xlog.h:63-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/brin_xlog.h#L63-L72)

## Overview
A WAL record structure that contains the information necessary for logging BRIN tuple insertion operations in the Write-Ahead Log.

## Definition

```c
typedef struct xl_brin_insert
{
	BlockNumber heapBlk;

	/* extra information needed to update the revmap */
	BlockNumber pagesPerRange;

	/* offset number in the main page to insert the tuple to. */
	OffsetNumber offnum;
} xl_brin_insert;
```
## Detailed Description
The  structure is used in PostgreSQL's WAL system to record the insertion of a new BRIN tuple. BRIN (Block Range Index) tuples contain summary information about ranges of heap blocks. This WAL record captures all the necessary information to replay the insertion operation during crash recovery.

The structure works in conjunction with backup blocks: backup block 0 contains the main page with the new BrinTuple data, and backup block 1 contains the revmap (reverse mapping) page. The revmap is a critical component of BRIN indexes that maps heap block ranges to their corresponding index tuples.

## Parameters / Member Variables
- : The heap block number that this BRIN tuple summarizes. This identifies which range of heap blocks the tuple provides summary information for
- : The number of pages per range for this BRIN index, needed to properly update the revmap during replay
- : The offset number within the main page where the new tuple should be inserted

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (type)
  - OffsetNumber (type)
- Called from (representative examples):
  - [brin_doinsert](../b/brin_doinsert.md) (in src/backend/access/brin/brin_pageops.c:428)
  - [brin_xlog_insert_update](../b/brin_xlog_insert_update.md) (in src/backend/access/brin/brin_xlog.c:47)
  - [brin_xlog_insert](../b/brin_xlog_insert.md) (in src/backend/access/brin/brin_xlog.c:126)
  - [brin_desc](../b/brin_desc.md) (in src/backend/access/rmgrdesc/brindesc.c:35)
  - SizeOfBrinInsert (macro in src/include/access/brin_xlog.h:74)
  - [xl_brin_update](xl_brin_update.md) (used as base for xl_brin_update structure)

## Notes and Other Information
- This structure is used with two backup blocks: the main page (block 0) and the revmap page (block 1)
- The revmap update is essential for maintaining the mapping between heap blocks and their corresponding BRIN index tuples
- The  macro calculates the size of this structure for WAL operations
- This structure serves as the base for the  structure, demonstrating the related nature of insert and update operations in BRIN indexes
- The offnum field ensures that the tuple is inserted at the correct location during WAL replay