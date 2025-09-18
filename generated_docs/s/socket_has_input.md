# socket_has_input

## Location
src/bin/pgbench/pgbench.c: 7957 - 7962

## Overview
Checks if a specific socket file descriptor is ready for input after a select() operation by testing its status in the socket set.

## Definition
static bool socket_has_input(socket_set *sa, int fd, int idx)

## Detailed Description
This function determines whether a specific socket file descriptor has input data available for reading. It works in conjunction with wait_on_socket_set() by checking the results of a previous select() call. The function uses the FD_ISSET macro to test if the specified file descriptor is set in the fd_set, which indicates that the socket has data available for reading without blocking. This is a simple wrapper around the standard FD_ISSET functionality, providing a more readable interface for pgbench's socket management.

## Parameters / Member Variables
- `sa`: Pointer to the socket_set containing the fd_set that was used in the select() call
- `fd`: The socket file descriptor to check for input readiness
- `idx`: Index parameter (currently unused in the implementation)

## Dependencies
- Functions called/Symbols referenced:
  - FD_ISSET (system macro)
- Called from (representative examples):
  - threadRun (for checking socket readiness after select() in pgbench)

## Notes and Other Information
- Returns true if the socket has input data available, false otherwise
- Should only be called after a successful wait_on_socket_set() call that returned > 0
- The idx parameter is unused in the current implementation but may be reserved for future use
- Part of the socket management trio: add_socket_to_set() adds sockets, wait_on_socket_set() waits for activity, socket_has_input() checks results
- Used in pgbench's main event loop to efficiently handle multiple concurrent database connections