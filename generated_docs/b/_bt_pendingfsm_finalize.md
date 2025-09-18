# _bt_pendingfsm_finalize

## Location
src/backend/access/nbtree/nbtpage.c: 2995 - 3061

## Overview
_bt_pendingfsm_finalize safely places newly deleted btree pages into the free space map at the end of a vacuum operation, when it can reliably determine which pages are safe to recycle.

## Definition
```c
void _bt_pendingfsm_finalize(Relation rel, BTVacState *vstate)
```

## Detailed Description
This function completes the deferred FSM update optimization by processing the array of pending deleted pages collected during vacuum. It performs a crucial safety check for each page: verifying that the page's safe transaction ID (safexid) is old enough that no concurrent transactions could still need the page data.

The function implements a two-phase safety protocol:
1. Updates the backend's XID horizon state by calling GetOldestNonRemovableTransactionId()
2. Uses GlobalVisCheckRemovableFullXid() to verify each page is truly safe to recycle

Pages are processed in safexid order (maintained by _bt_pendingfsm_add), allowing early termination when the first non-recyclable page is found, since all subsequent pages must also be non-recyclable.

The function includes debugging support (DEBUG_BTREE_PENDING_FSM) that introduces artificial delays to increase chances of successful page recycling in testing scenarios.

## Parameters / Member Variables
- `rel`: B-tree index relation being vacuumed
- `vstate`: BTVacState containing the array of pending pages to be processed, along with associated buffer management state

## Dependencies
- Functions called/Symbols referenced:
  - GetOldestNonRemovableTransactionId (XID horizon update)
  - GlobalVisCheckRemovableFullXid (safety check for page recycling)
  - RecordFreeIndexPage (adds page to FSM)
  - pfree (memory deallocation)
  - pg_usleep (debugging only)
- Structures referenced:
  - IndexBulkDeleteResult (vacuum statistics)
  - BTPendingFSM (pending page metadata)
  - FullTransactionId (transaction ID for safety checks)
- Called from (representative examples):
  - btvacuumscan

## Notes and Other Information
- Always frees the memory allocated by _bt_pendingfsm_init(), regardless of whether any pages were processed
- The optimization relies on other concurrent backends consuming XIDs to advance the global XID horizon
- Early termination optimization assumes pages are stored in safexid order, which is guaranteed by _bt_pendingfsm_add()
- Updates vacuum statistics (pages_free counter) for successfully recycled pages
- The DEBUG_BTREE_PENDING_FSM build option adds a 5-second sleep to improve testing effectiveness by allowing XID horizon advancement