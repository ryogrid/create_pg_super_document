# AtSubAbort_Portals

## Location
src/backend/utils/mmgr/portalmem.c: 979 - 1091

## Overview
Subtransaction abort handling function that deactivates and safely handles portals created or used during a failed subtransaction.

## Definition


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
  - hash_seq_init
  - hash_seq_search
  - MarkPortalFailed
  - ResourceOwnerNewParent
  - PointerIsValid
  - PortalReleaseCachedPlan
  - MemoryContextDeleteChildren
- Data types/Constants used:
  - SubTransactionId
  - ResourceOwner
  - HASH_SEQ_STATUS
  - PortalHashEnt
  - Portal
  - PORTAL_ACTIVE
  - PORTAL_FAILED
  - PORTAL_READY
- Called from:
  - AbortOutOfAnyTransaction (src/backend/access/transam/xact.c:4893)
  - AbortSubTransaction (src/backend/access/transam/xact.c:5258)

## Notes and Other Information
- This function does not destroy portal data structures; that is handled later in AtSubCleanup_Portals
- The function includes extensive logic to handle corner cases where portals might reference objects being destroyed during subtransaction rollback
- Resource owner manipulation ensures proper cleanup ordering and prevents assertion failures
- The function handles both portals created in the current subtransaction and those merely used in it
- Memory context cleanup is performed to release executor state and other subsidiary data
- The function is defined in src/backend/utils/mmgr/portalmem.c:979-1091