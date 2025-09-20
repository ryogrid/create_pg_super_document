# spgprocesspending

## Location
[src/backend/access/spgist/spgvacuum.c:692-803](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgvacuum.c#L692-L803)

## Overview
Processes the pending-TID list between pages during SP-GiST vacuum operations, handling redirections and expanding inner tuple downlinks.

## Definition

```c
static void
spgprocesspending(spgBulkDeleteState *bds)
```
## Detailed Description
The `spgprocesspending` function is a critical component of SP-GiST vacuum that processes a list of pending tuple identifiers (TIDs) accumulated during the main vacuum scan. It handles two primary scenarios:

1. **Leaf pages**: When a pending TID points to a leaf page, it vacuums the entire page and marks multiple pending items as done if they point to the same page for efficiency.

2. **Inner pages**: When a pending TID points to an inner tuple, it expands all downlinks from that tuple and adds them to the pending list for future processing. This handles the tree traversal aspect of SP-GiST vacuum.

The function implements an optimization where it processes all pending items pointing to the same page in a single visit, reducing I/O overhead. It also handles redirect tuples by following the redirection chain and adding the redirect target to the pending list.

## Parameters / Member Variables
- `bds`: Bulk delete state containing the pending list, vacuum statistics, index relation, and strategy information

## Dependencies
- Functions called/Symbols referenced:
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)/ItemPointerGetOffsetNumber
  - [ReadBufferExtended](../R/ReadBufferExtended.md)
  - [vacuumLeafPage](../v/vacuumLeafPage.md)
  - [vacuumRedirectAndPlaceholder](../v/vacuumRedirectAndPlaceholder.md)
  - [SpGistSetLastUsedPage](../S/SpGistSetLastUsedPage.md)
  - [spgAddPendingTID](spgAddPendingTID.md)
  - [spgClearPendingList](spgClearPendingList.md)
  - [PageIsNew](../P/PageIsNew.md)/SpGistPageIsDeleted/SpGistPageIsLeaf/SpGistBlockIsRoot
  - SGITITERATE
- Called from (representative examples):
  - [spgvacuumscan](spgvacuumscan.md)

## Notes and Other Information
- Implements an efficient batch processing approach by handling all pending items for the same page in one visit
- Uses exclusive buffer locking to ensure consistency during tuple examination
- Includes error checking to prevent invalid redirections to root pages
- Handles different tuple states (SPGIST_LIVE, SPGIST_REDIRECT) appropriately
- Clears the entire pending list after processing to prepare for the next cycle
- Critical for maintaining SP-GiST tree structure integrity during vacuum operations