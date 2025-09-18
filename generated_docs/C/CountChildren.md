# CountChildren

## Location
src/backend/postmaster/postmaster.c: 3880 - 3925

## Overview
CountChildren counts the number of child processes of specified types in the PostgreSQL postmaster, excluding dead_end children from the count.

## Definition
static int CountChildren(int target)

## Detailed Description
CountChildren iterates through the BackendList to count active child processes that match the specified target type criteria. The function serves as a key component in postmaster process management, allowing the postmaster to track how many processes of different types are currently running. This information is crucial for making decisions about process limits, shutdown procedures, and state transitions.

The function implements an optimization for the common case where all backend types are being counted (BACKEND_TYPE_ALL) by avoiding unnecessary shared memory access. For more specific type filtering, it dynamically updates the backend type classification for WAL sender processes that may have been recently announced, ensuring accurate counts even as process roles evolve during runtime.

Dead-end children (processes that failed during startup) are always excluded from counts to provide accurate metrics about functional processes only.

## Parameters / Member Variables
- target: Bitmask specifying which backend types to count (e.g., BACKEND_TYPE_ALL, BACKEND_TYPE_NORMAL, BACKEND_TYPE_WALSND)

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach (doubly-linked list iteration)
  - dlist_container (container_of macro for Backend structs)  
  - IsPostmasterChildWalSender (WAL sender process detection)
  - BACKEND_TYPE_ALL, BACKEND_TYPE_NORMAL, BACKEND_TYPE_WALSND (backend type constants)
- Called from (representative examples):
  - PostmasterStateMachine (for state transition decisions)
  - canAcceptConnections (connection limit checking)
  - SignalChildren (process management)

## Notes and Other Information
- Excludes dead_end children from all counts to focus on functional processes
- Implements performance optimization for BACKEND_TYPE_ALL queries
- Dynamically updates WAL sender process classifications during counting
- Uses bitmask-based filtering to support counting multiple backend types simultaneously
- Returns integer count of matching active child processes
- Critical for postmaster decision-making about process limits and shutdown sequences