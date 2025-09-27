# ConfigurePostmasterWaitSet

## Location
[src/backend/postmaster/postmaster.c:1603-1625](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L1603-L1625)

## Overview
Configures the postmaster's wait event set to control whether the server accepts new connections, rebuilding the event set based on the current operational state.

## Definition

```c
static void
ConfigurePostmasterWaitSet(bool accept_connections)
```
## Detailed Description
ConfigurePostmasterWaitSet manages the postmaster's event monitoring system by creating and configuring a WaitEventSet. This function is crucial for controlling the server's connection acceptance behavior during different operational phases:

- **During normal operation**: Creates a wait set that monitors both the postmaster latch and all listening sockets for incoming connections
- **During shutdown**: Creates a wait set that only monitors the postmaster latch, effectively stopping new connection acceptance while allowing existing operations to complete
- **During crash recovery**: Re-enables connection acceptance after system recovery

The function always destroys and recreates the entire WaitEventSet rather than modifying existing events, as PostgreSQL's event system doesn't currently support selective event removal. The wait set is automatically cleaned up in forked child processes by ClosePostmasterPorts().

## Parameters / Member Variables
- : Boolean flag determining whether to configure the wait set to accept new client connections

## Dependencies
- Functions called/Symbols referenced:
  - [FreeWaitEventSet](../F/FreeWaitEventSet.md)
  - [CreateWaitEventSet](CreateWaitEventSet.md)
  - [AddWaitEventToSet](../A/AddWaitEventToSet.md)
- Constants used:
  - WL_LATCH_SET
  - WL_SOCKET_ACCEPT
  - PGINVALID_SOCKET
- Global variables accessed:
  - pm_wait_set
  - MyLatch
  - NumListenSockets
  - ListenSockets
- Called from:
  - [ServerLoop](../S/ServerLoop.md)
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md)

## Notes and Other Information
- The function always includes the postmaster latch (MyLatch) in the wait set for internal signaling
- When accept_connections is true, all listening sockets from the ListenSockets array are added to monitor for incoming connections
- The WaitEventSet is dynamically sized: 1 event when not accepting connections, 1 + NumListenSockets when accepting
- This design allows for graceful shutdown by stopping new connections while preserving internal communication capabilities
- Child processes automatically clean up the wait set through ClosePostmasterPorts(), preventing resource leaks

## Simplified Source

```c
// Simplified version of ConfigurePostmasterWaitSet
static void ConfigurePostmasterWaitSet(bool accept_connections) {
    // Step 1: Clean up existing wait set
    if (pm_wait_set) {
        FreeWaitEventSet(pm_wait_set);
        pm_wait_set = NULL;
    }

    // Step 2: Create new wait set with appropriate size
    // Size = 1 (for latch) + NumListenSockets (if accepting connections)
    int wait_set_size = accept_connections ? (1 + NumListenSockets) : 1;
    pm_wait_set = CreateWaitEventSet(NULL, wait_set_size);

    // Step 3: Always add postmaster latch for internal signaling
    AddWaitEventToSet(pm_wait_set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);

    // Step 4: Optionally add listening sockets for new connections
    if (accept_connections) {
        for (int i = 0; i < NumListenSockets; i++) {
            AddWaitEventToSet(pm_wait_set, WL_SOCKET_ACCEPT, ListenSockets[i], NULL, NULL);
        }
    }
}
```

Key simplifications made:
- Added step-by-step comments explaining the logical flow
- Extracted wait_set_size calculation for clarity
- Emphasized the conditional nature of socket monitoring
- Focused on the core rebuild-and-configure pattern