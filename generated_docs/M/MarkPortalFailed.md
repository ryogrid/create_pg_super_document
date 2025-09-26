# MarkPortalFailed

## Location
[src/backend/utils/mmgr/portalmem.c:442-467](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L442-L467)

## Overview
Transitions a portal into FAILED state when execution encounters an error, ensuring proper cleanup and preventing further portal execution attempts.

## Definition
```c
void MarkPortalFailed(Portal portal)
```

## Detailed Description
MarkPortalFailed handles error scenarios during portal execution by transitioning the portal to a FAILED state. This function is critical for error recovery and ensures that failed portals cannot be executed further while performing necessary cleanup operations.

The function validates that the portal is not already in DONE state (since done portals should not transition to failed), then updates the status to FAILED. Similar to MarkPortalDone, it executes any registered cleanup hook to ensure proper resource cleanup. This cleanup is particularly important during transaction abort scenarios where the cleanup hook must be executed before the system reaches AtCleanup_Portals.

## Parameters / Member Variables
- `portal`: The Portal structure to transition to failed state. Must not be in DONE state.

## Dependencies
- Functions called/Symbols referenced:
  - PORTAL_DONE (constant for state validation)
  - PORTAL_FAILED (constant for setting failed state)
  - PointerIsValid (macro to check if cleanup function pointer is valid)
  - [cleanup](../c/cleanup.md) (portal cleanup function pointer)
- Called from (representative examples):
  - [PersistHoldablePortal](../P/PersistHoldablePortal.md) (src/backend/commands/portalcmds.c:467)
  - [PortalStart](../P/PortalStart.md) (src/backend/tcop/pquery.c:592)
  - [PortalRun](../P/PortalRun.md) (src/backend/tcop/pquery.c:809)
  - [AtAbort_Portals](../A/AtAbort_Portals.md) (src/backend/utils/mmgr/portalmem.c:797, 820)
  - [AtSubAbort_Portals](../A/AtSubAbort_Portals.md) (src/backend/utils/mmgr/portalmem.c:1019, 1053)

## Notes and Other Information
- The function enforces that portals must never have their status set to PORTAL_FAILED directly - this function must always be used to ensure proper cleanup execution
- Used extensively in transaction abort scenarios to mark portals as failed and clean up their resources
- The cleanup hook execution prevents resource leaks and maintains system consistency during error conditions
- After cleanup execution, the cleanup pointer is set to NULL to prevent double cleanup
- Located in src/backend/utils/mmgr/portalmem.c:442-467