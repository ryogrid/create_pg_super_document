# HoldPinnedPortals

## Location
[src/backend/utils/mmgr/portalmem.c:1207-1255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L1207-L1255)

## Overview
Converts all pinned portals to held cursors during transaction control operations (COMMIT/ROLLBACK) inside procedures to prevent their destruction during transaction shutdown.

## Definition
```c
void HoldPinnedPortals(void)
```

## Detailed Description
HoldPinnedPortals is a critical function for transaction control within stored procedures and PL/pgSQL functions. When a COMMIT or ROLLBACK is initiated inside a procedure, this function must be called to protect internally-generated cursors from being dropped during transaction shutdown.

The function iterates through all portals in the PortalHashTable and converts any pinned portals to held cursors by calling HoldPortal(). It marks these portals as "auto-held" so that exception handling knows to clean them up automatically. In normal execution paths, the procedural language is responsible for cleaning up these portals since transaction end will no longer handle it.

The function includes safety checks to ensure that only read-only cursors (SELECT statements) can be held during transaction control, preventing issues with UPDATE/DELETE cursors that have complex semantics during transaction control.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md): Initialize hash table iteration
  - [hash_seq_search](../h/hash_seq_search.md): Iterate through hash table entries
  - [HoldPortal](HoldPortal.md): Convert portal to held cursor
  - ereport/elog: Error reporting functions
- Data structures referenced:
  - [HASH_SEQ_STATUS](HASH_SEQ_STATUS.md): Hash table iteration status
  - [PortalHashEnt](../P/PortalHashEnt.md): Hash table entry for portals
  - PortalHashTable: Global hash table of all portals
  - [Portal](../P/Portal.md): Portal data structure
  - PORTAL_ONE_SELECT: Portal strategy constant
  - PORTAL_READY: Portal status constant
- Called from:
  - [_SPI_commit](../S/_SPI_commit.md): SPI commit operation
  - [_SPI_rollback](../S/_SPI_rollback.md): SPI rollback operation

## Notes and Other Information
- This function is automatically called by SPI, but procedural languages that initiate transaction control through other means must call it explicitly
- Only portals with PORTAL_ONE_SELECT strategy can be held, enforcing read-only semantics
- The function validates that portals are in PORTAL_READY state before attempting to hold them
- Auto-held portals require explicit cleanup by the procedural language in normal execution paths
- Exception handling will automatically clean up auto-held portals when exceptions occur

## Simplified Source

```c
void
HoldPinnedPortals(void)
{
    HASH_SEQ_STATUS status;
    PortalHashEnt *hentry;

    // Iterate through all portals in the hash table
    hash_seq_init(&status, PortalHashTable);

    while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL)
    {
        Portal portal = hentry->portal;

        // Process pinned portals that aren't already auto-held
        if (portal->portalPinned && !portal->autoHeld)
        {
            // Only read-only SELECT cursors can be held during transaction control
            if (portal->strategy != PORTAL_ONE_SELECT)
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("cannot perform transaction commands inside a cursor loop that is not read-only")));

            // Ensure portal is ready to be held
            if (portal->status != PORTAL_READY)
                elog(ERROR, "pinned portal is not ready to be auto-held");

            // Convert to held cursor and mark as auto-held
            HoldPortal(portal);
            portal->autoHeld = true;
        }
    }
}
```