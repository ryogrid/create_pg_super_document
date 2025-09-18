# PortalCleanup

## Location
[src/backend/commands/portalcmds.c:263-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/portalcmds.c#L263-L315)

## Overview
PortalCleanup is the standard cleanup hook for portals that properly shuts down executor resources when a portal is dropped or during error recovery.

## Definition
```c
void PortalCleanup(Portal portal)
```

## Detailed Description
PortalCleanup handles the proper cleanup of portal resources, particularly the executor state associated with running queries. It serves as the standard cleanup hook that is automatically called when portals are dropped or during error recovery. The function is designed to be safe to call during error abort scenarios and includes special handling for failed portals.

The function performs these key operations:
1. Validates that the portal is valid and that this function is indeed the registered cleanup hook
2. Checks if there is an active QueryDesc (query descriptor) that needs cleanup
3. For non-failed portals, properly shuts down the executor by calling ExecutorFinish and ExecutorEnd
4. Handles resource owner context switching to ensure cleanup occurs in the correct context
5. Frees the QueryDesc structure
6. Includes safety measures for error abort scenarios

The function is particularly careful during error abort situations (when portal->status is PORTAL_FAILED) to avoid operations that might themselves fail, relying instead on transaction abort mechanisms for resource cleanup.

## Parameters / Member Variables
- `portal`: Portal pointer to the portal being cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - PortalIsValid
  - [ExecutorFinish](../E/ExecutorFinish.md)
  - [ExecutorEnd](../E/ExecutorEnd.md)
  - [FreeQueryDesc](../F/FreeQueryDesc.md)
  - CurrentResourceOwner (global variable)
- Called from (representative examples):
  - PortalDrop (indirectly via portal cleanup mechanism)
  - [Portal](Portal.md) cleanup hooks during transaction abort

## Notes and Other Information
- This function is set as the default cleanup hook for portals in CreatePortal via portal->cleanup = PortalCleanup
- Special error handling ensures safety during transaction abort scenarios when portal->status is PORTAL_FAILED
- The function temporarily switches to the portal's resource owner context to ensure proper resource cleanup
- QueryDesc is reset to NULL early to prevent double cleanup attempts in case of errors
- The function is idempotent - it's safe to call multiple times on the same portal
- Transaction abort mechanisms will handle resource cleanup if this function itself fails during error scenarios