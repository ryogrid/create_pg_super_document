# PreCommit_Portals

## Location
src/backend/utils/mmgr/portalmem.c: 677 - 780

## Overview
PreCommit_Portals handles portal processing during transaction commit, converting holdable cursors to materialized form and cleaning up non-holdable portals.

## Definition
bool PreCommit_Portals(bool isPrepare)

## Detailed Description
PreCommit_Portals is a critical function called during transaction commit processing that manages all portals created in the current transaction. The function performs different actions based on portal type and state:

1. **Holdable Cursors**: Converts holdable cursors created in the current transaction to materialized form by calling HoldPortal(), allowing them to persist beyond transaction boundaries
2. **Non-holdable Portals**: Removes all non-holdable portals created in the current transaction via PortalDrop()
3. **Active Portals**: Handles special cases like multi-transaction utility commands (e.g., VACUUM) by cleaning up resources while preserving the portal
4. **Previous Transaction Portals**: Leaves portals from prior transactions untouched

The function also enforces constraints such as preventing PREPARE TRANSACTION when holdable cursors exist and ensuring no pinned portals remain during commit.

## Parameters / Member Variables
- isPrepare: Boolean flag indicating whether this is a PREPARE TRANSACTION (true) or regular COMMIT (false)

## Dependencies
- Functions called/Symbols referenced:
  - hash_seq_init
  - hash_seq_search
  - hash_seq_term
  - HoldPortal
  - PortalDrop
  - UnregisterSnapshotFromOwner
  - PORTAL_ACTIVE
  - PORTAL_READY
  - CURSOR_OPT_HOLD
  - InvalidSubTransactionId
- Called from (representative examples):
  - CommitTransaction
  - PrepareTransaction

## Notes and Other Information
- Returns true if any portals changed state (potentially triggering user-defined code), false otherwise
- Implements a restart mechanism during hash table iteration to handle cases where user-defined code might modify the portal hash table
- Prevents PREPARE TRANSACTION when holdable cursors exist due to unclear semantics
- Enforces that no pinned portals exist during commit (except auto-held ones)
- Properly manages resource owners and snapshots to prevent resource leaks
- Critical for maintaining PostgreSQL's transaction isolation and cursor management