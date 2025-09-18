# alloc_socket_set

## Location
src/bin/pgbench/pgbench.c: 7896 - 7901

## Overview
A function in pgbench that allocates and initializes a socket set data structure used for managing multiple socket connections during benchmark execution, with different implementations for ppoll() and select() based polling.

## Definition


## Detailed Description
This function provides a cross-platform abstraction for allocating socket sets that can be used with either ppoll() or select() system calls for monitoring multiple database connections. The implementation varies based on compile-time configuration:

**PPOLL version**: Allocates a variable-size structure containing an array of  elements, with  set to the requested count and  initialized to 0. This version supports higher socket counts and provides better performance.

**SELECT version**: Allocates a fixed-size structure containing an  and  field, ignoring the count parameter since select() has built-in limitations. This version is used as a fallback when ppoll() is not available.

The allocated socket set is used throughout pgbench's multi-threaded benchmark execution to efficiently wait for input on multiple database connections simultaneously.

## Parameters / Member Variables
- : The maximum number of sockets that the set should accommodate (used only in ppoll() version)

## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation function)
  -  type (structure definition varies by implementation)
  -  macro (ppoll version only)
- Called from (representative examples):
  - [threadRun](../t/threadRun.md)() function at src/bin/pgbench/pgbench.c:7438

## Notes and Other Information
- There are two distinct implementations depending on whether  or  is defined
- The ppoll() version allocates variable-sized memory based on the count parameter using a flexible array member
- The select() version ignores the count parameter and always allocates a fixed-size structure
- The ppoll() implementation uses  to calculate the exact memory needed for the structure plus the pollfd array
- Both versions use  to ensure the allocated memory is zero-initialized
- This is part of pgbench's socket abstraction layer that hides the differences between ppoll() and select() implementations
- The socket set allocated by this function must be freed using the corresponding  function
- ppoll() is preferred when available due to its higher ceiling on the number of usable sockets
- Each thread in pgbench allocates its own socket set to manage connections to database backends