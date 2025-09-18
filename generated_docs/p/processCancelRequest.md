# processCancelRequest

## Location
src/backend/postmaster/postmaster.c: 1837 - 1896

## Overview
Processes client cancel requests by locating the target backend process and sending a SIGINT signal to interrupt its current operation.

## Definition


## Detailed Description
processCancelRequest handles PostgreSQL's query cancellation mechanism, which allows clients to request the termination of long-running queries. The function implements a secure two-factor authentication system using both process ID and a secret cancellation key:

**Authentication Process:**
- Searches the active backend list for a process matching the provided PID
- Verifies the cancel authentication code matches the backend's stored cancel_key
- Only proceeds with cancellation if both PID and key match exactly

**Security Features:**
- Prevents unauthorized query cancellation by requiring the secret cancel key
- Logs security violations when wrong keys are provided for valid PIDs
- Reports attempts to cancel non-existent processes

**Implementation Variations:**
- **Non-EXEC_BACKEND**: Uses the postmaster's BackendList with dlist iteration
- **EXEC_BACKEND**: Accesses shared memory array ShmemBackendArray for process information

The function sends SIGINT to the target backend, which triggers PostgreSQL's standard query interruption handling. No response is sent back to the client, maintaining the protocol's fire-and-forget semantics.

## Parameters / Member Variables
- : Process ID of the backend to cancel
- : Secret authentication code that must match the backend's stored cancel key

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach (non-EXEC_BACKEND)
  - dlist_container (non-EXEC_BACKEND)
  - [MaxLivePostmasterChildren](../M/MaxLivePostmasterChildren.md) (EXEC_BACKEND)
  - [signal_child](../s/signal_child.md)
  - ereport
  - [errmsg_internal](../e/errmsg_internal.md)
  - [errmsg](../e/errmsg.md)
- Data structures accessed:
  - BackendList (non-EXEC_BACKEND)
  - ShmemBackendArray (EXEC_BACKEND)
  - [Backend](../B/Backend.md) structure fields (pid, cancel_key)
- Constants used:
  - DEBUG2, LOG (logging levels)
  - SIGINT (signal type)
- Called from:
  - [ProcessStartupPacket](../P/ProcessStartupPacket.md)

## Notes and Other Information
- The function supports two compilation modes (EXEC_BACKEND and non-EXEC_BACKEND) with different backend list access patterns
- Cancel keys are generated when backend processes start and stored in the Backend structure
- The SIGINT signal triggers standard PostgreSQL query cancellation, which sets the QueryCancelPending flag
- Security is critical - wrong cancel keys are logged as potential security incidents
- The function provides no feedback to clients, maintaining protocol simplicity
- [Backend](../B/Backend.md) processes handle the SIGINT signal through PostgreSQL's standard signal handling infrastructure
- This mechanism enables responsive query cancellation without requiring complex protocol extensions