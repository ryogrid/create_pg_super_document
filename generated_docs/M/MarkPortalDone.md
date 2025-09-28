# MarkPortalDone

## Location
[src/backend/utils/mmgr/portalmem.c:414-441](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L414-L441)

## Overview
Transitions a portal from ACTIVE to DONE state and performs necessary cleanup operations for completed portal execution.

## Definition
```c
void MarkPortalDone(Portal portal)
```

## Detailed Description
MarkPortalDone handles the completion of portal execution by transitioning the portal from ACTIVE to DONE state. This function is essential for proper portal lifecycle management and ensures that all necessary cleanup operations are performed when a portal finishes execution.

The function first validates that the portal is in the expected ACTIVE state using an assertion. After updating the status to DONE, it performs important cleanup by invoking any registered cleanup hook. This cleanup step is particularly critical in scenarios involving transaction rollbacks, where the cleanup hook must be executed before reaching AtCleanup_Portals to prevent system inconsistencies.

## Parameters / Member Variables
- `portal`: The Portal structure to transition to done state. Must be in ACTIVE state.

## Dependencies
- Functions called/Symbols referenced:
  - PORTAL_ACTIVE (constant for state validation)
  - PORTAL_DONE (constant for setting done state)
  - PointerIsValid (macro to check if cleanup function pointer is valid)
  - [cleanup](../c/cleanup.md) (portal cleanup function pointer)
- Called from (representative examples):
  - [PortalRun](../P/PortalRun.md) (src/backend/tcop/pquery.c:793)
  - PortalIsValid (src/include/utils/portal.h:235)

## Notes and Other Information
- The function enforces that portals must never have their status set to PORTAL_DONE directly - this function must always be used to ensure proper cleanup execution
- The cleanup hook execution is essential for preventing issues during transaction abort scenarios, particularly with ROLLBACK commands in already-aborted transactions
- After cleanup execution, the cleanup pointer is set to NULL to prevent double cleanup
- Located in src/backend/utils/mmgr/portalmem.c:414-441

## Simplified Source

```c
// Simplified version of MarkPortalDone
void MarkPortalDone(Portal portal) {
    // Validate that portal is in active state
    Assert(portal->status == PORTAL_ACTIVE);

    // Transition to done state
    portal->status = PORTAL_DONE;

    // Execute cleanup hook if present
    if (PointerIsValid(portal->cleanup)) {
        portal->cleanup(portal);
        portal->cleanup = NULL;
    }
}
```

Key simplifications made:
- Consolidated comments while preserving essential information
- Maintained the assertion for state validation
- Preserved the cleanup hook execution logic
- Focused on the core workflow: validate state, update status, run cleanup
- Kept the essential safety mechanisms for proper portal lifecycle management