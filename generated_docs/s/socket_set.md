# socket_set

## Location
src/bin/pgbench/pgbench.c: 108 - 112

## Overview
The socket_set structure is used in pgbench to manage a collection of file descriptors for efficient socket polling and monitoring operations.

## Definition


## Detailed Description
This structure wraps around the standard Unix fd_set mechanism to provide organized socket management for pgbench's concurrent connection handling. It maintains both the fd_set structure for use with select() system calls and tracks the maximum file descriptor number for optimization purposes. This design allows pgbench to efficiently monitor multiple database connections simultaneously during benchmark execution.

## Parameters / Member Variables
- : The largest file descriptor number currently stored in the fds set, used to optimize select() calls by providing the upper bound for file descriptor scanning
- : Standard Unix fd_set structure that holds the actual set of file descriptors to be monitored

## Dependencies
- Functions called/Symbols referenced:
  - fd_set (system type)
- Called from (representative examples):
  - threadRun
  - setalarm
  - alloc_socket_set
  - free_socket_set
  - clear_socket_set
  - add_socket_to_set
  - wait_on_socket_set
  - socket_has_input

## Notes and Other Information
This structure is specifically designed for pgbench's multi-threaded benchmark operations where multiple database connections need to be monitored concurrently. The socket_set provides a clean abstraction over the low-level fd_set operations, making the code more maintainable and portable across different Unix-like systems.