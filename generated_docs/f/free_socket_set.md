# free_socket_set

## Location
src/bin/pgbench/pgbench.c: 7902 - 7907

## Overview
A function in pgbench that deallocates memory for a socket set data structure, serving as the counterpart to alloc_socket_set() in the socket abstraction layer.

## Definition


## Detailed Description
This function provides a simple wrapper around  to deallocate memory for socket set structures. It is part of pgbench's socket abstraction layer that provides a consistent interface for managing socket sets regardless of whether the underlying implementation uses ppoll() or select().

The function is identical in both the ppoll() and select() implementations, as both rely on dynamic memory allocation through  in , requiring corresponding deallocation through .

This function should always be called to properly clean up socket set resources when they are no longer needed, preventing memory leaks in long-running benchmark sessions.

## Parameters / Member Variables
- : Pointer to the socket_set structure to be deallocated (allocated previously by alloc_socket_set())

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL's memory deallocation function)
  -  type (structure being deallocated)
- Called from (representative examples):
  - [threadRun](../t/threadRun.md)() function at src/bin/pgbench/pgbench.c:7726

## Notes and Other Information
- This function is identical in both POLL_USING_PPOLL and POLL_USING_SELECT implementations
- It serves as a proper counterpart to  for complete memory management
- The function does not perform any validation on the input pointer - it assumes the caller provides a valid socket_set pointer
- Called at the end of thread execution in threadRun() to clean up allocated socket set resources
- Part of pgbench's cross-platform socket abstraction that hides implementation differences between ppoll() and select()
- Essential for preventing memory leaks, especially in multi-threaded benchmark scenarios where each thread allocates its own socket set
- Uses PostgreSQL's standard memory management functions rather than direct malloc()/free() calls