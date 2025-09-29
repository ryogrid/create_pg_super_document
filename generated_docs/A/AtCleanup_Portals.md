# AtCleanup_Portals

## Location
[src/backend/utils/mmgr/portalmem.c:858-916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L858-L916)

## Overview
AtCleanup_Portals performs final cleanup after transaction abort by dropping all portals created in the failed transaction while preserving those from previous transactions.

## Definition
void AtCleanup_Portals(void)

## Detailed Description
AtCleanup_Portals is called during the final phase of transaction cleanup, after AtAbort_Portals has already performed initial abort processing. This function completes the portal cleanup by actually dropping portal data structures that belong to the failed transaction:

1. **Active Portals**: Skips active portals (from multi-transaction commands) to avoid interfering with ongoing operations
2. **Previous Transaction Portals**: Preserves portals from previous transactions and auto-held portals, as they should survive the current transaction's failure
3. **Pinned Portals**: Forcibly unpins any remaining pinned portals since their owners were interrupted by the abort
4. **Cleanup Hooks**: Skips any remaining cleanup hooks with a warning, as running user code during cleanup could be dangerous
5. **Portal Removal**: Drops all remaining portals created in the failed transaction using PortalDrop()

This function ensures complete cleanup while being safe to call during error recovery scenarios.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - PointerIsValid
  - [PortalDrop](../P/PortalDrop.md)
  - elog
  - PORTAL_ACTIVE
  - InvalidSubTransactionId
- Called from (representative examples):
  - [CleanupTransaction](../C/CleanupTransaction.md)

## Notes and Other Information
- Called after AtAbort_Portals as part of the complete transaction cleanup sequence
- Forcibly unpins portals since their original pinners were interrupted by the abort
- Issues warnings for portals with unrun cleanup hooks rather than executing potentially dangerous user code
- Uses Assert() statements to verify expected portal states during cleanup
- Critical for preventing portal leaks and ensuring clean system state after transaction failures
- Part of PostgreSQL's comprehensive error recovery and resource management system

## Simplified Source

```c
// Simplified version of AtCleanup_Portals
void AtCleanup_Portals(void) {
    HASH_SEQ_STATUS status;
    PortalHashEnt *hentry;

    // Iterate through all portals in the hash table
    hash_seq_init(&status, PortalHashTable);

    while ((hentry = hash_seq_search(&status)) != NULL) {
        Portal portal = hentry->portal;

        // Skip active portals (from multi-transaction commands)
        if (portal->status == PORTAL_ACTIVE)
            continue;

        // Skip portals from previous transactions or auto-held ones
        if (portal->createSubid == InvalidSubTransactionId || portal->autoHeld)
            continue;

        // Force unpin if still pinned (owner was interrupted by abort)
        if (portal->portalPinned)
            portal->portalPinned = false;

        // Skip cleanup hooks to avoid running user code during cleanup
        if (portal->cleanup != NULL) {
            elog(WARNING, "skipping cleanup for portal \"%s\"", portal->name);
            portal->cleanup = NULL;
        }

        // Drop the portal
        PortalDrop(portal, false);
    }
}
```

Key simplifications made:
- Removed detailed comments and consolidated to essential logic flow
- Simplified conditional checks while preserving safety logic
- Abstracted PointerIsValid() to simple NULL check for clarity
- Maintained all critical safety mechanisms (active portal checks, pinning logic)
- Preserved warning mechanism for skipped cleanup hooks
- Kept the essential hash table iteration pattern intact