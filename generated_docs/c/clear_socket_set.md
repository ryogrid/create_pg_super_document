# clear_socket_set

## Location
src/bin/pgbench/pgbench.c: 7908 - 7914

## Overview
A function in pgbench that resets a socket set to an empty state, allowing it to be reused for a new round of socket monitoring in the benchmark execution loop.

## Definition


## Detailed Description
This function provides a platform-specific implementation for clearing socket sets, with different behaviors depending on whether ppoll() or select() is used for socket monitoring:

**PPOLL version**: Simply resets the  counter to 0, effectively marking all previously added sockets as unused without clearing the actual pollfd array contents.

**SELECT version**: Calls  to clear the file descriptor set and resets  to -1, completely clearing all socket information from the fd_set.

The function is called as part of pgbench's socket monitoring loop to prepare for a new iteration of socket setup. After calling this function, sockets must be re-added using  before the next call to .

## Parameters / Member Variables
- : Pointer to the socket_set structure to be cleared

## Dependencies
- Functions called/Symbols referenced:
  - PPOLL version: None (just resets  field)
  - SELECT version:  macro,  field reset
  -  type (structure being cleared)
- Called from (representative examples):
  - threadRun() function at src/bin/pgbench/pgbench.c:7518
  - threadRun() function at src/bin/pgbench/pgbench.c:7620

## Notes and Other Information
- The implementations differ significantly between ppoll() and select() versions due to their different underlying data structures
- In the ppoll() version, the actual  array contents remain unchanged; only the count is reset for efficiency
- In the select() version, the entire  is zeroed and  is reset to -1 to indicate no file descriptors are set
- This function is part of the socket abstraction layer's state management cycle
- Called before each iteration of socket setup in the benchmark loop to ensure a clean starting state
- Must be followed by calls to  to repopulate the socket set with current active connections
- The function is designed for repeated use within pgbench's main event loop
- Essential for the "destructive" nature of socket monitoring mentioned in the API documentation, where socket sets must be cleared and rebuilt after each wait operation