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