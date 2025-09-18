# AtAbort_Portals

## Location
src/backend/utils/mmgr/portalmem.c: 781 - 857

## Overview
AtAbort_Portals handles portal cleanup during transaction abort, ensuring proper resource management and portal state transitions when transactions fail.

## Definition
void AtAbort_Portals(void)

## Detailed Description
AtAbort_Portals is called during transaction abort processing to handle all portals that were created or active in the failing transaction. The function performs careful cleanup while preserving portals that should survive the abort:

1. **Active Portals**: If a FATAL error is in progress, marks active portals as failed to prevent executor shutdown issues
2. **Previous Transaction Portals**: Leaves portals from previous transactions completely untouched
3. **Auto-held Portals**: Preserves auto-held cursors as they are managed externally
4. **Ready Portals**: Marks READY portals as failed since they may reference objects from the failed transaction
5. **Cleanup Hooks**: Executes any registered cleanup functions before releasing resources
6. **Resource Management**: Releases cached plans, clears resource owners, and deletes subsidiary memory contexts

The function is designed to be safe during error conditions and ensures that portal state remains consistent even when transactions fail unexpectedly.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - hash_seq_init
  - hash_seq_search
  - MarkPortalFailed
  - PointerIsValid
  - PortalReleaseCachedPlan
  - MemoryContextDeleteChildren
  - PORTAL_ACTIVE
  - PORTAL_READY
  - InvalidSubTransactionId
- Called from (representative examples):
  - AbortTransaction
  - AbortOutOfAnyTransaction

## Notes and Other Information
- Does not delete portal data structures themselves, only cleans up subsidiary resources
- Uses shmem_exit_inprogress to detect FATAL error conditions and adjust behavior accordingly
- Preserves active portals' memory contexts to avoid issues during error handling
- Critical for preventing resource leaks and maintaining system stability during transaction failures
- Cleanup hooks are called before any resource deallocation to ensure proper state cleanup
- Part of PostgreSQL's robust error recovery and transaction abort mechanisms