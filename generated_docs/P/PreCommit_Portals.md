# PreCommit_Portals

## Location
[src/backend/utils/mmgr/portalmem.c:677-780](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L677-L780)

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
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [hash_seq_term](../h/hash_seq_term.md)
  - [HoldPortal](../H/HoldPortal.md)
  - [PortalDrop](PortalDrop.md)
  - [UnregisterSnapshotFromOwner](../U/UnregisterSnapshotFromOwner.md)
  - PORTAL_ACTIVE
  - PORTAL_READY
  - CURSOR_OPT_HOLD
  - InvalidSubTransactionId
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md)
  - [PrepareTransaction](PrepareTransaction.md)

## Notes and Other Information
- Returns true if any portals changed state (potentially triggering user-defined code), false otherwise
- Implements a restart mechanism during hash table iteration to handle cases where user-defined code might modify the portal hash table
- Prevents PREPARE TRANSACTION when holdable cursors exist due to unclear semantics
- Enforces that no pinned portals exist during commit (except auto-held ones)
- Properly manages resource owners and snapshots to prevent resource leaks
- Critical for maintaining PostgreSQL's transaction isolation and cursor management

## Simplified Source

```c
bool PreCommit_Portals(bool isPrepare)
{
    bool result = false;
    HASH_SEQ_STATUS status;
    PortalHashEnt *hentry;

    hash_seq_init(&status, PortalHashTable);

    while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL) {
        Portal portal = hentry->portal;

        // Check for improperly pinned portals
        if (portal->portalPinned && !portal->autoHeld)
            elog(ERROR, "cannot commit while a portal is pinned");

        // Handle active portals (e.g., VACUUM, procedure commits)
        if (portal->status == PORTAL_ACTIVE) {
            // Clean up resources but preserve the portal
            if (portal->holdSnapshot) {
                if (portal->resowner)
                    UnregisterSnapshotFromOwner(portal->holdSnapshot, portal->resowner);
                portal->holdSnapshot = NULL;
            }
            portal->resowner = NULL;
            portal->portalSnapshot = NULL;
            continue;
        }

        // Handle holdable cursors created in current transaction
        if ((portal->cursorOptions & CURSOR_OPT_HOLD) &&
            portal->createSubid != InvalidSubTransactionId &&
            portal->status == PORTAL_READY) {

            // Reject PREPARE TRANSACTION with holdable cursors
            if (isPrepare)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot PREPARE a transaction that has created a cursor WITH HOLD")));

            // Convert to materialized form for persistence
            HoldPortal(portal);
            result = true;
        }
        // Skip portals from previous transactions
        else if (portal->createSubid == InvalidSubTransactionId) {
            continue;
        }
        // Drop all non-holdable portals from current transaction
        else {
            PortalDrop(portal, true);
            result = true;
        }

        // Restart iteration due to potential hash table changes from user code
        hash_seq_term(&status);
        hash_seq_init(&status, PortalHashTable);
    }

    return result;
}
```