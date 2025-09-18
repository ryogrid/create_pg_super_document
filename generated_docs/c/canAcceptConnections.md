# canAcceptConnections

## Location
src/backend/postmaster/postmaster.c: 1897 - 1956

## Overview
Determines whether the PostgreSQL server can accept new connections of a specified type based on current system state, operational mode, and resource limitations.

## Definition


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
- : Type of backend connection being requested (BACKEND_TYPE_NORMAL, BACKEND_TYPE_AUTOVAC, or BACKEND_TYPE_BGWORKER)

## Dependencies
- Functions called/Symbols referenced:
  - CountChildren
  - MaxLivePostmasterChildren
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
  - BackendStartup
  - StartAutovacuumWorker
  - assign_backendlist_entry
  - BackendMain

## Notes and Other Information
- The function implements a defense-in-depth approach with multiple validation layers
- Background workers have special treatment as their startup logic is handled elsewhere
- Connection limits are soft limits during authentication to handle race conditions gracefully
- Smart shutdown allows maintenance connections while blocking user connections
- The exact MaxBackends limit is enforced later when joining the shared-inval backend array
- Different return codes enable callers to provide appropriate error messages to clients
- Hot standby mode allows read-only connections while the primary recovery is in progress
- The function must coordinate with MaxLivePostmasterChildren() for proper resource management