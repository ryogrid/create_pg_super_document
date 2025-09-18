# pgwin32_accept

## Location
[src/backend/port/win32/socket.c:337-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/socket.c#L337-L358)

## Overview
PostgreSQL's Windows-specific wrapper function for accepting socket connections that integrates signal handling and provides proper error handling for Windows platforms.

## Definition
```c
SOCKET pgwin32_accept(SOCKET s, struct sockaddr *addr, int *addrlen)
```

## Detailed Description
pgwin32_accept is a wrapper around the Windows WSAAccept() function that provides PostgreSQL-specific functionality. Unlike the simpler wrapper functions, this one includes signal polling via pgwin32_poll_signals() before attempting the accept operation. This ensures that PostgreSQL can handle signals properly during blocking accept operations on Windows. The function uses WSAAccept() instead of the standard accept() function and translates Windows socket errors into PostgreSQL's error handling system.

## Parameters / Member Variables
- `s`: Listening socket descriptor from which to accept connections
- `addr`: Pointer to sockaddr structure to receive the connecting client's address (can be NULL)
- `addrlen`: Pointer to integer containing the size of the addr buffer, updated with actual address size

## Dependencies
- Functions called/Symbols referenced:
  - [pgwin32_poll_signals](pgwin32_poll_signals.md)
  - WSAAccept (Windows socket API)
  - [TranslateSocketError](../T/TranslateSocketError.md)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- This function is part of PostgreSQL's Windows socket abstraction layer located in src/backend/port/win32/socket.c
- Unlike other pgwin32 socket wrappers, this function includes signal polling to ensure proper signal handling during blocking operations
- Uses WSAAccept() instead of accept() for enhanced Windows socket functionality
- Returns INVALID_SOCKET on error after proper error translation
- The comment indicates that EINTR handling is deliberately avoided as it's not handled in pqcomm.c