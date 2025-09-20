# AtSubAbort_Portals

## Location
[src/backend/utils/mmgr/portalmem.c:979-1091](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L979-L1091)

## Overview
Subtransaction abort handling function that deactivates and safely handles portals created or used during a failed subtransaction.

## Definition

```c
structure proper, we can
		 * release any memory in subsidiary contexts, such as executor state.
		 * The cleanup hook was the last thing that might have needed data
		 * there.
		 */
		MemoryContextDeleteChildren(portal->portalContext);
```
## Detailed Description
AtSubAbort_Portals handles the cleanup of portals when a subtransaction aborts. The function deactivates portals that were created or used during the failed subtransaction, ensuring system stability by preventing access to potentially corrupted or invalid portal states. Unlike AtSubCommit_Portals, this function focuses on safe deactivation rather than ownership transfer.

The function performs several critical operations:
1. For portals created in other subtransactions but used in the current one, it updates the activeSubid to the parent and handles special cases for ACTIVE portals by marking them as FAILED.
2. For portals created in the current subtransaction, it forces them into FAILED state to prevent crashes from references to objects that may be destroyed during rollback.
3. It calls cleanup hooks, releases cached plans, clears resource owners, and deletes subsidiary memory contexts to ensure clean resource management.

The function ensures that no portal remains in an inconsistent state that could cause crashes during subsequent operations.

## Parameters
- : The subtransaction ID of the subtransaction being aborted
- : The subtransaction ID of the parent subtransaction
- : The resource owner of the subtransaction being aborted
- : The resource owner of the parent transaction context

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - MarkPortalFailed
  - [ResourceOwnerNewParent](../R/ResourceOwnerNewParent.md)
  - PointerIsValid
  - [PortalReleaseCachedPlan](../P/PortalReleaseCachedPlan.md)
  - [MemoryContextDeleteChildren](../M/MemoryContextDeleteChildren.md)
- Data types/Constants used:
  - SubTransactionId
  - ResourceOwner
  - HASH_SEQ_STATUS
  - PortalHashEnt
  - [Portal](../P/Portal.md)
  - PORTAL_ACTIVE
  - PORTAL_FAILED
  - PORTAL_READY
- Called from:
  - [AbortOutOfAnyTransaction](AbortOutOfAnyTransaction.md) (src/backend/access/transam/xact.c:4893)
  - [AbortSubTransaction](AbortSubTransaction.md) (src/backend/access/transam/xact.c:5258)

## Notes and Other Information
- This function does not destroy portal data structures; that is handled later in AtSubCleanup_Portals
- The function includes extensive logic to handle corner cases where portals might reference objects being destroyed during subtransaction rollback
- Resource owner manipulation ensures proper cleanup ordering and prevents assertion failures
- The function handles both portals created in the current subtransaction and those merely used in it
- Memory context cleanup is performed to release executor state and other subsidiary data
- The function is defined in src/backend/utils/mmgr/portalmem.c:979-1091