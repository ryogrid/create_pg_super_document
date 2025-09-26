# MarkPortalActive

## Location
[src/backend/utils/mmgr/portalmem.c:395-413](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L395-L413)

## Overview
Transitions a portal from READY to ACTIVE state, ensuring proper state management and subtransaction tracking for PostgreSQL's portal execution system.

## Definition

```c
void
MarkPortalActive(Portal portal)
```
## Detailed Description
MarkPortalActive performs a critical state transition that marks a portal as actively executing. This function enforces strict state validation, ensuring that only portals in the READY state can be activated. The function performs a runtime check rather than just an assertion to guarantee safety, and will raise an ERROR if the portal is not in the prerequisite READY state.

Upon successful validation, the function updates the portal's status to ACTIVE and records the current subtransaction ID. This subtransaction tracking is essential for proper cleanup and rollback behavior, allowing PostgreSQL to correctly manage portal lifecycles within nested transactions.

## Parameters / Member Variables
- : The Portal structure to transition to active state. Must be in READY state.

## Dependencies
- Functions called/Symbols referenced:
  - PORTAL_READY (constant for ready state check)
  - PORTAL_ACTIVE (constant for setting active state) 
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md) (to record current subtransaction)
- Called from (representative examples):
  - [PersistHoldablePortal](../P/PersistHoldablePortal.md) (src/backend/commands/portalcmds.c:351)
  - [PortalRun](../P/PortalRun.md) (src/backend/tcop/pquery.c:717)
  - [PortalRunFetch](../P/PortalRunFetch.md) (src/backend/tcop/pquery.c:1396)

## Notes and Other Information
- The function enforces that portals must never have their status set to PORTAL_ACTIVE directly - this function must always be used to ensure proper subtransaction tracking
- Runtime validation prevents execution of portals that are not ready, maintaining system integrity
- The activeSubid field assignment enables proper cleanup during subtransaction abort scenarios
- Located in src/backend/utils/mmgr/portalmem.c:395-413