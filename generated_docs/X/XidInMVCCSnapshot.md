# XidInMVCCSnapshot

## Location
src/backend/utils/time/snapmgr.c: 1856 - 1954

## Overview
Determines whether a given transaction ID is still in-progress according to the rules of a specific MVCC snapshot.

## Definition


## Detailed Description
XidInMVCCSnapshot is a core function in PostgreSQL's MVCC (Multi-Version Concurrency Control) implementation that determines transaction visibility. It checks whether a specific transaction ID should be considered as 'still running' from the perspective of a given snapshot. This function implements the complex logic needed to handle both normal operations and recovery scenarios, dealing with transaction ID overflow conditions and the distinction between top-level transactions and subtransactions.

The function uses range checks for optimization, followed by searches through the snapshot's transaction ID arrays. It handles different storage formats used during recovery versus normal operation, and manages subtransaction-to-parent transaction ID conversion when needed.

## Parameters / Member Variables
- : The transaction ID to check for visibility in the snapshot
- : The MVCC snapshot containing the transaction visibility information

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdPrecedes (for XID ordering comparison)
  - TransactionIdFollowsOrEquals (for XID ordering comparison)
  - pg_lfind32 (for searching XID arrays)
  - SubTransGetTopmostTransaction (for converting subXIDs to top-level XIDs)
- Called from (representative examples):
  - HeapTupleSatisfiesMVCC (tuple visibility checking)
  - find_inheritance_children_extended (inheritance tree traversal)
  - asyncQueueProcessPageEntries (async notification processing)
  - RelationGetPartitionDesc (partition descriptor access)

## Notes and Other Information
- The function never reports the current backend's own transactions as 'running' since they are not stored in snapshots
- Uses quick range checks (xmin/xmax) to eliminate most XIDs without array searches for performance
- Handles snapshot overflow conditions by converting subtransaction XIDs to their parent XIDs
- During recovery, all XIDs are stored in the subxip array with the xip array being empty
- Critical for PostgreSQL's MVCC visibility rules and transaction isolation guarantees