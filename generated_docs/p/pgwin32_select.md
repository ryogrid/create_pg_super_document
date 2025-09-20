# pgwin32_select

## Location
[src/backend/port/win32/socket.c:517-706](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/socket.c#L517-L706)

## Overview
Windows-specific implementation of the select() system call that provides PostgreSQL-compatible socket multiplexing with signal handling support on Windows platforms.

## Definition

```c
int
pgwin32_select(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timeval *timeout)
```
## Detailed Description
 is a Windows-specific implementation of the POSIX select() system call, designed to provide socket multiplexing functionality for PostgreSQL on Windows. Unlike the standard POSIX select(), this implementation uses Windows Socket API events (, ) to efficiently monitor multiple sockets for I/O readiness.

The function integrates closely with PostgreSQL's Windows signal handling system, allowing signals to interrupt socket operations properly. It implements a sophisticated approach to handle Windows socket behavior quirks, particularly around write-readiness detection which requires special handling due to Windows socket event logging behavior.

Key features:
- Uses Windows Socket events for efficient multiplexing
- Integrates with PostgreSQL's signal handling via 
- Implements special write-readiness detection with dummy send operations
- Supports timeout functionality using Windows wait primitives
- Does not implement exceptfds checking (not used in PostgreSQL)
- Handles both blocking and timeout scenarios

## Parameters / Member Variables
- : Maximum file descriptor number plus 1 (ignored on Windows, kept for POSIX compatibility)
- : Pointer to fd_set containing sockets to monitor for read readiness (input/output parameter)
- : Pointer to fd_set containing sockets to monitor for write readiness (input/output parameter)  
- : Pointer to fd_set for exception conditions (must be NULL - not implemented)
- : Pointer to timeval structure specifying maximum wait time, or NULL for infinite wait

## Dependencies
- Functions called/Symbols referenced:
  - : Check for and handle pending PostgreSQL signals
  - : Windows API for sending data (used for write-readiness testing)
  - : Create Windows socket event objects
  - : Associate socket with event and specify network events of interest
  - : Wait for multiple Windows objects to become signaled
  - : Retrieve network events that occurred on a socket
  - : Close Windows event handle
  - : Get Windows socket error codes
  - : Convert Windows socket errors to PostgreSQL errno values
  - : Process queued PostgreSQL signals
  - : POSIX error code for interrupted system call
- Called from (representative examples):
  - No direct references found in the current codebase (likely used via macro substitution or function pointer)

## Notes and Other Information
- Windows-only function (part of )
- Function signature matches POSIX select() for compatibility but ignores  parameter
- Does NOT implement exceptfds functionality - will assert if exceptfds is not NULL
- Implements Windows-specific workaround for write-readiness detection by performing dummy send operations
- Uses  as maximum event array size to handle worst-case scenario of completely different read and write socket sets
- Integrates with PostgreSQL's signal system via  
- Includes comprehensive error handling and cleanup of Windows event objects
- Supports both finite timeouts (converted to milliseconds) and infinite waits
- Returns number of ready sockets on success, 0 on timeout, -1 on error/interruption
- Part of PostgreSQL's Windows socket abstraction layer that provides POSIX-like semantics on Windows