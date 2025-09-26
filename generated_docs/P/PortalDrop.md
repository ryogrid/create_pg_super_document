# PortalDrop

## Location
[src/backend/utils/mmgr/portalmem.c:468-606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L468-L606)

## Overview
Destroys a portal completely, performing comprehensive cleanup of all associated resources including memory contexts, tuple stores, snapshots, and cached plans.

## Definition
```c
void PortalDrop(Portal portal, bool isTopCommit)
```

## Detailed Description
PortalDrop is the primary function for completely destroying a portal and cleaning up all its associated resources. This function handles the complex task of properly releasing memory contexts, tuple stores, snapshots, cached plans, and resource owners while maintaining system integrity across different transaction states.

The function performs extensive validation to ensure portals cannot be dropped while pinned or active. It handles cleanup in a carefully ordered sequence: first executing any registered cleanup hooks, then removing the portal from the hash table, releasing cached plans, handling snapshots, managing resource owners based on transaction context, and finally cleaning up tuple stores and memory contexts.

The isTopCommit parameter influences resource cleanup behavior, particularly for resource owner handling and lock management during different transaction commit/abort scenarios.

## Parameters / Member Variables
- `portal`: The Portal structure to destroy. Must be valid and not pinned or active.
- `isTopCommit`: Boolean indicating whether this is being called during a top-level transaction commit.

## Dependencies
- Functions called/Symbols referenced:
  - PortalIsValid (validation of portal structure)
  - PORTAL_ACTIVE (constant for active state check)
  - PointerIsValid (macro to check cleanup function pointer)
  - PortalHashTableDelete (remove portal from hash table)
  - [PortalReleaseCachedPlan](PortalReleaseCachedPlan.md) (release cached execution plan)
  - [UnregisterSnapshotFromOwner](../U/UnregisterSnapshotFromOwner.md) (unregister snapshot from resource owner)
  - PORTAL_FAILED (constant for failed state check)
  - [ResourceOwnerRelease](../R/ResourceOwnerRelease.md) (release resources in multiple phases)
  - [ResourceOwnerDelete](../R/ResourceOwnerDelete.md) (delete resource owner)
  - [tuplestore_end](../t/tuplestore_end.md) (cleanup tuple store)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (delete memory contexts)
- Called from (representative examples):
  - [PerformPortalClose](PerformPortalClose.md) (src/backend/commands/portalcmds.c:249)
  - [ExecuteQuery](../E/ExecuteQuery.md) (src/backend/commands/prepare.c:257)
  - [exec_simple_query](../e/exec_simple_query.md) (src/backend/tcop/postgres.c:1288)
  - [AtCleanup_Portals](../A/AtCleanup_Portals.md) (src/backend/utils/mmgr/portalmem.c:906)
  - [PortalHashTableDeleteAll](PortalHashTableDeleteAll.md) (src/backend/utils/mmgr/portalmem.c:624)

## Notes and Other Information
- Enforces that pinned portals cannot be dropped, preventing premature cleanup of portals still needed by other components
- Prevents dropping of active portals to maintain execution integrity
- Resource cleanup behavior varies based on transaction context (top commit, sub commit, abort scenarios)
- For ordinary portal drops of non-FAILED portals, locks are transferred to the transaction's ResourceOwner rather than released immediately
- Handles cross-transaction tuple stores by explicitly deleting their temporary files
- Early removal from hash table prevents infinite error-recovery loops during subsequent cleanup failures
- Located in src/backend/utils/mmgr/portalmem.c:468-606