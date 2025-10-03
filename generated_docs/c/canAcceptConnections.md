# canAcceptConnections

## Location
[src/backend/postmaster/postmaster.c:1897-1956](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L1897-L1956)

## Overview
Determines whether the PostgreSQL server can accept new connections of a specified type based on current system state, operational mode, and resource limitations.

## Definition

```c
static CAC_state
canAcceptConnections(int backend_type)
```
## Detailed Description
canAcceptConnections serves as the gatekeeper for all new database connections, implementing a comprehensive state-based access control system. The function evaluates multiple criteria to determine connection acceptability:

**System State Validation:**
- **Startup phase**: Rejects normal connections during system initialization (CAC_STARTUP)
- **Recovery states**: Handles crash recovery (CAC_RECOVERY) and consistency waiting (CAC_NOTCONSISTENT)
- **Shutdown**: Prevents new connections when shutdown is initiated (CAC_SHUTDOWN)
- **Operational**: Allows connections in normal run state (PM_RUN) and hot standby (PM_HOT_STANDBY)

**Backend Type Differentiation:**
- **BACKEND_TYPE_NORMAL**: Subject to all restrictions including smart shutdown rules
- **BACKEND_TYPE_AUTOVAC**: Follows most rules but bypasses smart shutdown restrictions
- **BACKEND_TYPE_BGWORKER**: Exempted from most state checks (bgworker_should_start_now() handles their logic)

**Resource Management:**
- Enforces connection limits based on MaxLivePostmasterChildren()
- Allows slight over-subscription to account for authentication failures and concurrent exits
- Returns CAC_TOOMANY when the child process limit is reached

**Smart Shutdown Handling:**
- Respects the connsAllowed flag for graceful shutdowns
- Only applies to normal user connections, not maintenance processes

## Parameters / Member Variables
- `backend_type`: Type of backend connection being requested (BACKEND_TYPE_NORMAL, BACKEND_TYPE_AUTOVAC, or BACKEND_TYPE_BGWORKER)
## Dependencies
- Functions called/Symbols referenced:
  - [CountChildren](../C/CountChildren.md)
  - [MaxLivePostmasterChildren](../M/MaxLivePostmasterChildren.md)
- Return values/Constants used:
  - CAC_OK (connections allowed)
  - CAC_SHUTDOWN (shutdown in progress)
  - CAC_STARTUP (system starting up)
  - CAC_NOTCONSISTENT (recovery not consistent)
  - CAC_RECOVERY (crash recovery mode)
  - CAC_TOOMANY (too many connections)
- Global variables accessed:
  - pmState (postmaster state)
  - Shutdown (shutdown status)
  - FatalError (error state flag)
  - connsAllowed (smart shutdown flag)
- Called from:
  - [BackendStartup](../B/BackendStartup.md)
  - [StartAutovacuumWorker](../S/StartAutovacuumWorker.md)
  - [assign_backendlist_entry](../a/assign_backendlist_entry.md)
  - [BackendMain](../B/BackendMain.md)

## Notes and Other Information
- The function implements a defense-in-depth approach with multiple validation layers
- Background workers have special treatment as their startup logic is handled elsewhere
- Connection limits are soft limits during authentication to handle race conditions gracefully
- Smart shutdown allows maintenance connections while blocking user connections
- The exact MaxBackends limit is enforced later when joining the shared-inval backend array
- Different return codes enable callers to provide appropriate error messages to clients
- Hot standby mode allows read-only connections while the primary recovery is in progress
- The function must coordinate with MaxLivePostmasterChildren() for proper resource management

## Simplified Source

```c
// Simplified version of canAcceptConnections
static CAC_state canAcceptConnections(int backend_type) {
    CAC_state result = CAC_OK;

    // Check if system is in a state that can accept connections
    // Skip this check for background workers (they have their own logic)
    if (pmState != PM_RUN && pmState != PM_HOT_STANDBY &&
        backend_type != BACKEND_TYPE_BGWORKER) {

        // Return appropriate state based on current condition
        if (Shutdown > NoShutdown)
            return CAC_SHUTDOWN;        // Shutdown in progress
        else if (!FatalError && pmState == PM_STARTUP)
            return CAC_STARTUP;         // System starting up
        else if (!FatalError && pmState == PM_RECOVERY)
            return CAC_NOTCONSISTENT;   // Recovery not yet consistent
        else
            return CAC_RECOVERY;        // Crash recovery mode
    }

    // Smart shutdown: block normal connections but allow maintenance
    if (!connsAllowed && backend_type == BACKEND_TYPE_NORMAL)
        return CAC_SHUTDOWN;

    // Check resource limits: don't exceed maximum children
    if (CountChildren(BACKEND_TYPE_ALL) >= MaxLivePostmasterChildren())
        result = CAC_TOOMANY;

    return result;
}
```

Key simplifications made:
- Consolidated comments for better readability
- Simplified conditional logic explanations
- Maintained the essential decision tree structure
- Preserved all critical state checks and return values
- Focused on the main execution path without losing functionality