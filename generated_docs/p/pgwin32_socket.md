# pgwin32_socket

## Location
[src/backend/port/win32/socket.c:291-314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/socket.c#L291-L314)

## Overview
pgwin32_socket is a Windows-specific socket creation function that creates sockets configured for overlapped I/O operations and non-blocking mode, providing the foundation for PostgreSQLs asynchronous socket operations on Windows.

## Definition
```c
SOCKET pgwin32_socket(int af, int type, int protocol)
```

## Detailed Description
This function serves as PostgreSQLs Windows-specific socket creation wrapper that addresses Windows platform requirements for efficient socket operations. It creates sockets using WSASocket with the WSA_FLAG_OVERLAPPED flag, enabling asynchronous I/O operations that are essential for scalable server applications.

After creating the overlapped socket, it immediately configures it for non-blocking operation using ioctlsocket with FIONBIO. This combination of overlapped and non-blocking modes allows PostgreSQL to implement efficient event-driven socket handling on Windows platforms.

The function includes proper error handling, translating Windows socket errors to POSIX equivalents and ensuring cleanup of partially created sockets when configuration fails.

## Parameters / Member Variables
- `af`: Address family (e.g., AF_INET for IPv4, AF_INET6 for IPv6)
- `type`: Socket type (e.g., SOCK_STREAM for TCP, SOCK_DGRAM for UDP)  
- `protocol`: Protocol specification (usually 0 for default protocol)
- `SOCKET`: Valid socket handle on success
- `INVALID_SOCKET`: On failure (errno is set via TranslateSocketError)

## Dependencies
- Functions called/Symbols referenced:
  - WSASocket (Winsock API for creating overlapped sockets)
  - WSA_FLAG_OVERLAPPED (flag for overlapped I/O capability)
  - ioctlsocket (Winsock API for socket I/O control)
  - FIONBIO (I/O control code for non-blocking mode)
  - [TranslateSocketError](../T/TranslateSocketError.md) (error translation function)
  - closesocket (Winsock API for socket cleanup)

- Called from:
  - Not directly referenced in the analyzed codebase (likely called from higher-level PostgreSQL socket creation routines)

## Notes and Other Information
- This is a Windows-specific function located in src/backend/port/win32/socket.c
- Creates sockets with both overlapped I/O and non-blocking characteristics
- Essential for PostgreSQLs high-performance socket operations on Windows
- Properly handles cleanup by closing socket if non-blocking configuration fails  
- Sets errno to 0 on successful completion
- Part of PostgreSQLs Windows socket abstraction layer that provides POSIX-like socket behavior
- The overlapped flag is crucial for efficient Windows socket event handling and I/O completion ports