# PortalStatus

## Location
src/include/utils/portal.h: 111 - 112

## Overview
PortalStatus is an enumeration that defines the possible execution states of a portal throughout its lifecycle, enabling proper state management and transitions during query execution.

## Definition
```c
typedef enum PortalStatus
{
    PORTAL_NEW,         /* freshly created */
    PORTAL_DEFINED,     /* PortalDefineQuery done */
    PORTAL_READY,       /* PortalStart complete, can run it */
    PORTAL_ACTIVE,      /* portal is running (can't delete it) */
    PORTAL_DONE,        /* portal is finished (don't re-run it) */
    PORTAL_FAILED,      /* portal got error (can't re-run it) */
} PortalStatus;
```

## Detailed Description
PortalStatus represents the execution state machine for portals in PostgreSQL. It enforces a strict lifecycle progression that ensures proper resource management and prevents invalid operations. The status transitions generally follow a forward progression (NEW → DEFINED → READY → ACTIVE → DONE/FAILED), with the notable exception that a portal can transition from ACTIVE back to READY if query execution is interrupted before completion. This state management is crucial for maintaining data consistency, proper resource cleanup, and preventing race conditions during concurrent operations. The status also serves as a safety mechanism to prevent attempts to execute completed or failed portals.

## Parameters / Member Variables
- `PORTAL_NEW`: Initial state when a portal is first created but not yet defined with a query
- `PORTAL_DEFINED`: State after PortalDefineQuery has been called to associate a query with the portal
- `PORTAL_READY`: State after PortalStart has been called, indicating the portal is prepared for execution
- `PORTAL_ACTIVE`: State during query execution, preventing portal deletion and indicating active resource usage
- `PORTAL_DONE`: Terminal state after successful query completion, preventing re-execution
- `PORTAL_FAILED`: Terminal state after an error occurred during execution, preventing re-execution

## Dependencies
- Functions called/Symbols referenced:
  - No direct symbol references (this is an enum definition)
- Called from (representative examples):
  - PortalData (as the status field)
  - Portal state management functions
  - Query execution routines

## Notes and Other Information
- The status serves as a critical safety mechanism preventing invalid portal operations
- Transition from ACTIVE back to READY is the only backward transition allowed
- DONE and FAILED are terminal states - portals cannot be reused after reaching these states
- The status is checked extensively throughout the portal management code to ensure valid operations
- State transitions are typically managed by specific portal management functions
- The status helps coordinate resource cleanup and prevents memory leaks
- Error handling relies on the FAILED status to prevent retry attempts on corrupted portals
- The status is essential for proper transaction management and rollback operations