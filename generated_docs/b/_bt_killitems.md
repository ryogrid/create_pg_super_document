# _bt_killitems

## Location
[src/backend/access/nbtree/nbtutils.c:4171-4366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L4171-L4366)

## Overview
Marks index tuples as dead (LP_DEAD) based on kill list information from index scan operations, optimizing future scans by marking tuples that reference deleted heap rows.

## Definition


## Detailed Description
This function implements the "kill tuple" optimization for B-tree indexes, which marks index tuples as dead when the scan has determined that their corresponding heap tuples have been deleted. The function processes a list of killed items maintained in the scan state, matching them by heap TID to ensure correctness. It handles both regular tuples and posting list tuples (which contain multiple heap TIDs). The function includes sophisticated logic to handle concurrent modifications: if the page was pinned continuously since reading, no LSN check is needed; if the pin was dropped, it re-reads the page and verifies the LSN hasn't changed to ensure safety. When tuples are successfully marked dead, it sets the BTP_HAS_GARBAGE flag on the page to indicate cleanup is needed.

## Parameters / Member Variables
- : Index scan descriptor containing scan state and killed items list

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_lockbuf](_bt_lockbuf.md)/_bt_unlockbuf
  - [_bt_getbuf](_bt_getbuf.md)/_bt_relbuf
  - [BufferGetLSNAtomic](../B/BufferGetLSNAtomic.md)
  - [PageGetItemId](../P/PageGetItemId.md)/PageGetItem
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md)
  - [BTreeTupleGetNPosting](../B/BTreeTupleGetNPosting.md)/BTreeTupleGetPostingN
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
  - ItemIdIsDead/ItemIdMarkDead
  - BTPageGetOpaque
  - [MarkBufferDirtyHint](../M/MarkBufferDirtyHint.md)
- Called from (representative examples):
  - [btrescan](btrescan.md)
  - [btendscan](btendscan.md)
  - [btrestrpos](btrestrpos.md)
  - [_bt_steppage](_bt_steppage.md)

## Notes and Other Information
- Critical optimization for reducing index bloat and improving scan performance
- Handles posting list tuples by checking all heap TIDs within the posting list
- Uses LSN checking to ensure safety when page pins were dropped between read and kill operations
- Sets BTP_HAS_GARBAGE flag to trigger eventual cleanup by VACUUM
- Only marks items as dead if they aren't already marked, avoiding redundant WAL logging
- Part of PostgreSQL's B-tree maintenance and optimization system
- Located in src/backend/access/nbtree/nbtutils.c:4171-4366