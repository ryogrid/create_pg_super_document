# xl_btree_delete

## Location
src/include/access/nbtxlog.h: 239 - 254

## Overview
WAL record structure representing deletion and update operations on B-tree index pages during single-page cleanup, used to log both complete item deletions and partial posting list updates.

## Definition


## Detailed Description
The xl_btree_delete structure is a WAL record format used to log B-tree leaf page modifications during single-page cleanup operations. This record type handles two distinct operations simultaneously: complete deletion of index tuples and partial updates of posting list tuples (removing some but not all heap TIDs from a posting list). The structure is designed to support recovery conflict resolution on standby servers, particularly during logical decoding operations.

Unlike vacuum operations, this record type is used during regular index maintenance and preserves vacuum cycle IDs. The payload contains variable-length data including arrays of offset numbers for both deleted and updated items, followed by metadata for the updated tuples.

## Parameters / Member Variables
- : Transaction ID used for recovery conflict resolution on standby servers
- : Number of index tuples to be completely deleted from the page
- : Number of posting list tuples to be partially updated (some TIDs removed)
- : Boolean flag indicating if this is a catalog relation, used for logical decoding conflict handling

## Dependencies
- Functions called/Symbols referenced:
  - xl_btree_update (embedded in payload)
  - SizeOfBtreeDelete (size calculation macro)
- Called from (representative examples):
  - _bt_delitems_delete (src/backend/access/nbtree/nbtpage.c:1347)
  - btree_xlog_delete (src/backend/access/nbtree/nbtxlog.c:654)
  - btree_desc (src/backend/access/rmgrdesc/nbtdesc.c:72)

## Notes and Other Information
- The record's payload contains three distinct sections in order: deleted item offset numbers, updated item offset numbers, and xl_btree_update metadata items
- This structure differs from vacuum deletion records by preserving vacuum cycle IDs and using different conflict horizon semantics
- During recovery, the snapshotConflictHorizon and isCatalogRel fields are used together to resolve conflicts with logical decoding on standby servers
- The BTP_HAS_GARBAGE page flag is cleared after applying this operation
- Used specifically for single-page cleanup operations, not multi-page B-tree maintenance