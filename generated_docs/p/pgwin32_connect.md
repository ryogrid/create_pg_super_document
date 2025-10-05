# pgwin32_connect

## Location
[src/backend/port/win32/socket.c:359-381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/socket.c#L359-L381)

## Overview
PostgreSQL's Windows-specific wrapper function for establishing socket connections that handles non-blocking connection establishment with proper signal delivery and error handling.

## Definition
```c
int pgwin32_connect(SOCKET s, const struct sockaddr *addr, int addrlen)
```

## Detailed Description
pgwin32_connect is a sophisticated wrapper around the Windows WSAConnect() function that handles asynchronous connection establishment. The function initiates a connection using WSAConnect() and if it doesn't complete immediately (WSAEWOULDBLOCK), it uses pgwin32_waitforsinglesocket() to wait for the connection to complete while still allowing signal delivery. This approach ensures that PostgreSQL can handle signals properly during potentially long connection establishment phases on Windows systems.

## Parameters / Member Variables
- `s`: Socket descriptor to use for the connection
- `addr`: Pointer to sockaddr structure containing the target address to connect to
- `addrlen`: Length of the address structure

## Dependencies
- Functions called/Symbols referenced:
  - WSAConnect (Windows socket API)
  - WSAGetLastError (Windows socket API)
  - [TranslateSocketError](../T/TranslateSocketError.md)
  - [pgwin32_waitforsinglesocket](pgwin32_waitforsinglesocket.md)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- This function is part of PostgreSQL's Windows socket abstraction layer located in src/backend/port/win32/socket.c
- The comment indicates that no signal delivery occurs during the actual connect operation
- Handles non-blocking connection establishment by waiting with pgwin32_waitforsinglesocket()
- Uses WSAConnect() instead of the standard connect() function for enhanced Windows functionality
- The waiting loop continues indefinitely while signals are being delivered, ensuring proper signal handling
- Returns 0 on success, -1 on error after proper error translation

## Simplified Source

```c
int
pgwin32_connect(SOCKET s, const struct sockaddr *addr, int addrlen)
{
    int r;

    // Attempt connection using Windows socket API
    r = WSAConnect(s, addr, addrlen, NULL, NULL, NULL, NULL);
    if (r == 0)
        return 0;  // Connection succeeded immediately

    // Handle non-blocking connection
    if (WSAGetLastError() != WSAEWOULDBLOCK) {
        TranslateSocketError();
        return -1;
    }

    // Wait for connection to complete, allowing signal delivery
    while (pgwin32_waitforsinglesocket(s, FD_CONNECT, INFINITE) == 0) {
        // Continue waiting while signals are delivered
    }

    return 0;
}
```