# Backend

## Location
[src/backend/postmaster/postmaster.c:177-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L177-L219)

## Overview
Backend is a struct that represents information about a PostgreSQL backend process, used by the postmaster to track and manage child backend processes.

## Definition


## Detailed Description
The Backend struct is a core data structure in PostgreSQL's postmaster process that maintains information about each backend process. It serves as the primary means for the postmaster to track, identify, and manage child backend processes. Each backend process gets an entry in the BackendList when it starts, and this entry is used for process management, signal handling, and cleanup operations.

The struct is essential for PostgreSQL's multi-process architecture, allowing the postmaster to coordinate with backends for operations like query cancellation, shutdown procedures, and resource management.

## Parameters / Member Variables
- : Process ID of the backend process, used for system-level process management and signaling
- : Random key used for secure query cancellation requests to prevent unauthorized cancellation
- : Index into the PMChildSlot array for this backend, or -1 if not assigned
- : Type of backend process (regular backend, autovacuum worker, etc.)
- : Flag indicating if this backend will send an error message and terminate immediately
- : Flag indicating if this backend should receive background worker start/stop notifications
- : Doubly-linked list node for inclusion in the global BackendList

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_head](../d/dlist_head.md) (for BackendList management)
  - DLIST_STATIC_INIT (for list initialization)
  - [BackgroundWorker](BackgroundWorker.md) (related background worker structure)
- Called from (representative examples):
  - [processCancelRequest](../p/processCancelRequest.md) (for query cancellation)
  - [CleanupBackend](../C/CleanupBackend.md) (for process cleanup)
  - [BackendStartup](BackendStartup.md) (for new backend registration)
  - [assign_backendlist_entry](../a/assign_backendlist_entry.md) (for backend list management)

## Notes and Other Information
- The Backend struct is central to PostgreSQL's process management in the postmaster
- Under EXEC_BACKEND builds, backends are also stored in shared memory via ShmemBackendArray
- The cancel_key provides security by requiring knowledge of the key to cancel queries
- The dead_end flag is used for backends that will immediately terminate after sending an error
- [Backend](Backend.md) entries are maintained in a global BackendList for efficient iteration and management