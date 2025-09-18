# AtSubCleanup_Portals

## Location
src/backend/utils/mmgr/portalmem.c: 1092 - 1130

## Overview
Post-subtransaction abort cleanup function that drops all portals created in the failed subtransaction.

## Definition


## Detailed Description
AtSubCleanup_Portals performs the final cleanup phase for portals after a subtransaction abort. This function completes the portal cleanup process that was initiated by AtSubAbort_Portals by actually dropping (destroying) all portals that were created within the failed subtransaction. This function only targets portals that were originally created in the specified subtransaction and have not been reassigned to a parent transaction.

The function handles several critical aspects of portal cleanup:
1. It iterates through all portals in the system and identifies those created in the failed subtransaction
2. For pinned portals, it forcibly unpins them since the code that pinned them was interrupted by the abort
3. It skips any remaining cleanup hooks with a warning, since calling user-defined code during error recovery could be dangerous
4. Finally, it calls PortalDrop to completely destroy the portal and free all associated resources

This function ensures that no portal data structures remain after a subtransaction abort, preventing memory leaks and stale references.

## Parameters
- : The subtransaction ID of the subtransaction that failed and is being cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - PointerIsValid
  - elog
  - PortalDrop
- Data types used:
  - SubTransactionId
  - HASH_SEQ_STATUS
  - PortalHashEnt
  - [Portal](../P/Portal.md)
- Called from:
  - [CleanupSubTransaction](../C/CleanupSubTransaction.md) (src/backend/access/transam/xact.c:5331)

## Notes and Other Information
- This function is called during the cleanup phase after subtransaction abort, following AtSubAbort_Portals
- The function only drops portals created in the specified subtransaction; portals that were reassigned to parent transactions by AtSubCommit_Portals are preserved
- Forcible unpinning of portals is necessary because the abort interrupts normal portal lifecycle management
- User-defined cleanup hooks are skipped with a warning to avoid potential crashes during error recovery
- The function ensures complete cleanup by calling PortalDrop with the 'false' parameter for each affected portal
- The function is defined in src/backend/utils/mmgr/portalmem.c:1092-1130