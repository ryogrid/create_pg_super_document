# isDataGram

## Location
[src/backend/port/win32/socket.c:169-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/socket.c#L169-L180)

## Overview
isDataGram is a static utility function that determines whether a given Windows socket is of datagram type (UDP) by querying the sockets type option.

## Definition
```c
static int isDataGram(SOCKET s)
```

## Detailed Description
This function provides a way to programmatically determine if a socket uses datagram protocol (UDP) versus stream protocol (TCP). It uses the getsockopt() system call with SO_TYPE to retrieve the socket type and compares it against SOCK_DGRAM. The function handles error conditions by defaulting to assuming datagram type when getsockopt() fails, which may represent a conservative approach for certain use cases.

The function is essential for socket operations that need to behave differently based on the underlying transport protocol, as datagram and stream sockets have different characteristics regarding connection state, reliability, and data boundaries.

## Parameters / Member Variables
- `s`: The Windows socket (SOCKET type) to check
- `1`: If the socket is a datagram socket (SOCK_DGRAM) or if getsockopt() fails
- `0`: If the socket is not a datagram socket

## Dependencies
- Functions called/Symbols referenced:
  - getsockopt (standard socket API function)
  - SOL_SOCKET (socket level constant)
  - SO_TYPE (socket type option constant)
  - SOCK_DGRAM (datagram socket type constant)

- Called from (representative examples):
  - [pgwin32_waitforsinglesocket](../p/pgwin32_waitforsinglesocket.md)

## Notes and Other Information
- This is a Windows-specific function located in src/backend/port/win32/socket.c
- Returns 1 (true) when getsockopt() fails, which may be a defensive programming choice
- Used to differentiate behavior between connection-oriented (TCP) and connectionless (UDP) protocols
- The function assumes that if socket type cannot be determined, treating it as datagram is safer
- Essential for PostgreSQLs cross-platform socket abstraction layer on Windows