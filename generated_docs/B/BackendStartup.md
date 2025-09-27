# BackendStartup

## Location
[src/backend/postmaster/postmaster.c:3545-3641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L3545-L3641)

## Overview
Creates and launches a new backend process to handle a client connection, managing process creation, resource allocation, and backend registration in the PostgreSQL postmaster.

## Definition
static int BackendStartup(ClientSocket *client_sock)

## Detailed Description
This function orchestrates the creation of a new backend process to serve a client connection. It allocates and initializes a Backend data structure, generates a unique cancel key for the connection, determines if the backend should be a dead_end process based on connection acceptance status, assigns child slots for resource tracking, and uses postmaster_child_launch() to fork the actual process. On successful fork, it registers the new backend in the BackendList and shared memory structures. The function includes comprehensive error handling for memory allocation failures, cancel key generation failures, and fork failures, ensuring proper cleanup and client notification in all error cases.

## Parameters / Member Variables
- `client_sock`: Pointer to ClientSocket structure containing the client connection information and socket descriptor

## Dependencies
- Functions called/Symbols referenced:
  - [palloc_extended](../p/palloc_extended.md) (memory allocation with no-OOM option)
  - [RandomCancelKey](../R/RandomCancelKey.md) (generates unique cancel key)
  - [canAcceptConnections](../c/canAcceptConnections.md) (checks if new connections are allowed)
  - [AssignPostmasterChildSlot](../A/AssignPostmasterChildSlot.md) (assigns child slot for tracking)
  - [postmaster_child_launch](../p/postmaster_child_launch.md) (performs actual process forking)
  - [ReleasePostmasterChildSlot](../R/ReleasePostmasterChildSlot.md) (releases slot on failure)
  - [report_fork_failure_to_client](../r/report_fork_failure_to_client.md) (notifies client of fork failure)
  - [dlist_push_head](../d/dlist_push_head.md) (adds backend to list)
  - [ShmemBackendArrayAdd](../S/ShmemBackendArrayAdd.md) (shared memory registration, EXEC_BACKEND only)
- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md) (main postmaster loop when accepting connections)

## Notes and Other Information
- Returns STATUS_OK on success, STATUS_ERROR on failure
- Static function internal to postmaster.c
- Creates both live backends and dead_end backends depending on system state
- Dead_end backends are created when the system cannot accept new connections but still needs to handle the client socket gracefully
- The cancel key mechanism allows clients to cancel running queries
- Child slot assignment enables resource tracking and management
- Comment suggests considering StartAutovacuumWorker when modifying this code due to similar patterns
- EXEC_BACKEND conditional compilation affects shared memory handling
- Part of PostgreSQL's connection handling and process management infrastructure

## Simplified Source

```c
// Simplified version of BackendStartup
static int BackendStartup(ClientSocket *client_sock) {
    Backend *backend;
    pid_t child_pid;
    BackendStartupData startup_data;

    // Step 1: Allocate backend structure
    backend = palloc_extended(sizeof(Backend), MCXT_ALLOC_NO_OOM);
    if (!backend) {
        ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        return STATUS_ERROR;
    }

    // Step 2: Generate unique cancel key for this connection
    if (!RandomCancelKey(&MyCancelKey)) {
        pfree(backend);
        ereport(LOG, (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("could not generate random cancel key")));
        return STATUS_ERROR;
    }

    // Step 3: Configure backend based on connection acceptance state
    startup_data.canAcceptConnections = canAcceptConnections(BACKEND_TYPE_NORMAL);
    backend->dead_end = (startup_data.canAcceptConnections != CAC_OK);
    backend->cancel_key = MyCancelKey;

    // Step 4: Assign child slot for resource tracking (if not dead_end)
    if (!backend->dead_end) {
        backend->child_slot = AssignPostmasterChildSlot();
    } else {
        backend->child_slot = 0;
    }

    // Step 5: Initialize other backend fields
    backend->bgworker_notify = false;

    // Step 6: Fork the new backend process
    child_pid = postmaster_child_launch(B_BACKEND,
                                       (char *) &startup_data, sizeof(startup_data),
                                       client_sock);
    if (child_pid < 0) {
        // Fork failed - cleanup and report error
        if (!backend->dead_end) {
            ReleasePostmasterChildSlot(backend->child_slot);
        }
        pfree(backend);
        ereport(LOG, (errmsg("could not fork new process for connection: %m")));
        report_fork_failure_to_client(client_sock, errno);
        return STATUS_ERROR;
    }

    // Step 7: Fork succeeded - register the new backend
    ereport(DEBUG2, (errmsg_internal("forked new backend, pid=%d socket=%d",
                                    (int) child_pid, (int) client_sock->sock)));

    backend->pid = child_pid;
    backend->bkend_type = BACKEND_TYPE_NORMAL;
    dlist_push_head(&BackendList, &backend->elem);

#ifdef EXEC_BACKEND
    if (!backend->dead_end) {
        ShmemBackendArrayAdd(backend);
    }
#endif

    return STATUS_OK;
}
```

Key simplifications made:
- Renamed variables for clarity (bn → backend, pid → child_pid)
- Added step-by-step comments explaining the main logic flow
- Consolidated error handling patterns for readability
- Removed detailed errno handling in favor of clearer error flow
- Abstracted platform-specific details with conditional compilation note
- Focused on the main execution path and key decision points