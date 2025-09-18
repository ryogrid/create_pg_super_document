# disconnect_all

## Location
src/bin/pgbench/pgbench.c: 4720 - 4731

## Overview
Closes all database connections for a given array of client states by calling the finishCon function on each client connection.

## Definition
```c
static void disconnect_all(CState *state, int length)
```

## Detailed Description
This function provides a convenient way to cleanly disconnect all database connections in pgbench. It iterates through an array of CState structures, each representing a client connection state, and calls the finishCon function to properly close each connection. This is typically used during pgbench shutdown or cleanup operations to ensure all database connections are properly terminated and resources are freed.

## Parameters / Member Variables
- `state`: Pointer to an array of CState structures representing client connection states
- `length`: Integer specifying the number of elements in the state array

## Dependencies
- Functions called/Symbols referenced:
  - [finishCon](../f/finishCon.md) (connection cleanup function)
  - [CState](../C/CState.md) (client state structure type)
- Called from (representative examples):
  - [main](../m/main.md) (at src/bin/pgbench/pgbench.c:7410)
  - [threadRun](../t/threadRun.md) (at src/bin/pgbench/pgbench.c:7714)

## Notes and Other Information
- This is a static function, only accessible within pgbench.c
- Simple utility function that abstracts the common pattern of closing multiple connections
- Used in both the main function and thread cleanup contexts
- Ensures proper cleanup of database resources during pgbench termination
- The function assumes that the state array contains valid CState structures for the specified length