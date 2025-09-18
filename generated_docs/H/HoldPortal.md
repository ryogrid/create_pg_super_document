# HoldPortal

## Location
[src/backend/utils/mmgr/portalmem.c:636-676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L636-L676)

## Overview
HoldPortal prepares a portal for access by later transactions by making it holdable across transaction boundaries.

## Definition
static void HoldPortal(Portal portal)

## Detailed Description
HoldPortal is a static function that converts a regular portal into a holdable portal, allowing it to persist beyond the current transaction. The function performs several critical operations to prepare the portal for cross-transaction access:

1. Creates a hold store for the portal data
2. Persists the holdable portal state
3. Releases cached plan references
4. Transfers resource ownership away from the current transaction
5. Marks the portal as no longer belonging to the current transaction

This function is essential for implementing holdable cursors in PostgreSQL, which allow query results to remain accessible even after the transaction that created them has committed.

## Parameters / Member Variables
- portal: The Portal structure to be made holdable

## Dependencies
- Functions called/Symbols referenced:
  - [PortalCreateHoldStore](../P/PortalCreateHoldStore.md)
  - [PersistHoldablePortal](../P/PersistHoldablePortal.md)
  - [PortalReleaseCachedPlan](../P/PortalReleaseCachedPlan.md)
  - InvalidSubTransactionId
- Called from (representative examples):
  - [PreCommit_Portals](../P/PreCommit_Portals.md)
  - HoldPinnedPortals

## Notes and Other Information
- This is a static function, only accessible within portalmem.c
- The function sets the portal's resowner to NULL, transferring resource cleanup responsibility to transaction-wide cleanup
- Both createSubid and activeSubid are set to InvalidSubTransactionId to indicate the portal no longer belongs to the current transaction
- The createLevel is reset to 0 as part of the holdable conversion process
- Critical for implementing PostgreSQL's holdable cursor functionality