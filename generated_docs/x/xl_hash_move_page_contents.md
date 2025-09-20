# xl_hash_move_page_contents

## Location
[src/include/access/hash_xlog.h:136-142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/hash_xlog.h#L136-L142)

## Overview
A PostgreSQL WAL record structure that captures the information needed to replay tuple movement operations during hash index bucket squeeze operations.

## Definition

```c
typedef struct xl_hash_move_page_contents
{
	uint16		ntups;
	bool		is_prim_bucket_same_wrt;	/* true if the page to which
											 * tuples are moved is same as
											 * primary bucket page */
} xl_hash_move_page_contents;
```
## Detailed Description
The  structure is used for  WAL records, which log tuple movement operations that occur during hash index squeeze operations. Squeeze operations are part of hash index maintenance that help consolidate tuples and reclaim space by moving tuples from overflow pages back to primary bucket pages or between overflow pages.

This operation is typically triggered when tuples are deleted from a hash index, creating opportunities to consolidate the remaining tuples and potentially free up overflow pages. The squeeze operation helps maintain the hash index's efficiency by reducing the length of overflow chains.

The record works with three backup blocks that capture the complete state of the tuple movement:
- Backup Block 0: The bucket page (primary bucket page)
- Backup Block 1: The page containing the moved tuples (destination page)
- Backup Block 2: The page from which tuples were removed (source page)

The structure tracks both the number of tuples moved and whether the destination page is the same as the primary bucket page, which affects how the operation is replayed during recovery.

## Parameters / Member Variables
- : The number of tuples being moved from the source page to the destination page during the squeeze operation
- : Boolean flag indicating whether the destination page (where tuples are moved to) is the same as the primary bucket page; this affects WAL replay logic and space management decisions

## Dependencies
- Functions called/Symbols referenced:
  - uint16 (type)
  - [bool](../b/bool.md) (type)
- Called from (representative examples):
  - [hash_xlog_move_page_contents](../h/hash_xlog_move_page_contents.md) (WAL replay function)
  - [_hash_squeezebucket](../h/_hash_squeezebucket.md) (bucket squeeze implementation)
  - [hash_desc](../h/hash_desc.md) (WAL record description function)  
  - SizeOfHashMovePageContents (macro for size calculation)

## Notes and Other Information
- Part of PostgreSQL's hash index space reclamation and optimization system
- The squeeze operation helps reduce overflow chain length and improve index performance
- The  flag is crucial for proper WAL replay as it determines page update strategies
- Defined in src/include/access/hash_xlog.h:136-142
- Typically occurs after tuple deletions create opportunities for consolidation
- Works with three backup blocks to ensure complete capture of the tuple movement operation
- The operation can move tuples either back to primary pages or between overflow pages
- Critical for maintaining hash index efficiency by managing overflow page usage