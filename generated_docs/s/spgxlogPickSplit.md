# spgxlogPickSplit

## Location
[src/include/access/spgxlog.h:165-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgxlog.h#L165-L197)

## Overview
The spgxlogPickSplit struct is a PostgreSQL WAL (Write-Ahead Logging) record structure used to log pick-split operations in SP-GiST (Space-Partitioned Generalized Search Tree) indexes, which occurs when a leaf page becomes full and needs to be split.

## Definition


## Detailed Description
This structure represents a WAL record for SP-GiST pick-split operations, one of the most complex operations in SP-GiST index maintenance. A pick-split occurs when a leaf page becomes full and needs to be reorganized by creating a new inner node and potentially redistributing tuples between the original page and a new destination page. The struct contains all the necessary information to redo this operation during WAL replay, including which tuples to delete and insert, page initialization flags, and the new inner tuple structure.

## Parameters / Member Variables
- : Indicates whether this is a root page split operation
- : Number of tuples to delete from the source page
- : Number of tuples to insert on source and/or destination pages
- : Flag indicating whether to re-initialize the source page
- : Flag indicating whether to re-initialize the destination page
- : Offset number where the new inner tuple should be placed
- : Flag indicating whether to re-initialize the inner page
- : Flag indicating whether the pages are in the nulls tree portion of the index
- : Flag indicating whether the parent page is the same as the inner page
- : Offset number for the parent downlink location
- : Node index for the parent relationship
- : SP-GiST state information containing transaction ID and build flag
- : Flexible array member containing variable-length data including deleted/inserted tuple numbers, page selectors, new inner tuple, and leaf tuples

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