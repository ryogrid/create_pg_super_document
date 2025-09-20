# xl_btree_update

## Location
[src/include/access/nbtxlog.h:264-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtxlog.h#L264-L269)

## Overview
WAL record metadata structure describing partial updates to B-tree posting list tuples, specifying which TIDs within a posting list are to be deleted while preserving the tuple.

## Definition

```c
typedef struct xl_btree_update
{
	uint16		ndeletedtids;

	/* POSTING LIST uint16 OFFSETS TO A DELETED TID FOLLOW */
} xl_btree_update;
```
## Detailed Description
The xl_btree_update structure is a metadata descriptor used within WAL records to represent partial updates to B-tree posting list tuples. When some (but not all) heap TIDs need to be removed from an existing posting list tuple, this structure describes which specific TIDs should be deleted. The structure header contains only the count of TIDs to delete, followed immediately by an array of uint16 offsets identifying the positions within the original posting list.

This structure is embedded within larger WAL records (such as xl_btree_delete and xl_btree_vacuum) and is used during both regular cleanup operations and vacuum processing. The offsets stored are 0-based positions within the original posting list tuple, not page-level offset numbers.

## Parameters / Member Variables
- : Number of TIDs to be deleted from the posting list tuple
- Following the struct header: Array of uint16 offsets (0-based) into the original posting list indicating which TIDs should be removed

## Dependencies
- Functions called/Symbols referenced:
  - SizeOfBtreeUpdate (size calculation macro)
  - BTVacuumPosting (data structure for processing)
- Called from (representative examples):
  - [_bt_delitems_update](../b/_bt_delitems_update.md) (src/backend/access/nbtree/nbtpage.c:1443)
  - [btree_xlog_updates](../b/btree_xlog_updates.md) (src/backend/access/nbtree/nbtxlog.c:558, 591)
  - [btree_xlog_vacuum](../b/btree_xlog_vacuum.md) (src/backend/access/nbtree/nbtxlog.c:622, 626)
  - [btree_xlog_delete](../b/btree_xlog_delete.md) (src/backend/access/nbtree/nbtxlog.c:687, 691)

## Notes and Other Information
- The offsets following the struct header are 0-based positions within the original posting list, not page offset numbers
- Multiple xl_btree_update structures can appear in sequence within a single WAL record's payload
- Used in conjunction with _bt_update_posting() function to create modified posting list tuples
- The page offset number for the tuple being updated comes from the parent WAL record structure
- [Variable](../V/Variable.md)-length structure due to the trailing array of TID offsets
- Critical for maintaining posting list integrity during partial tuple updates