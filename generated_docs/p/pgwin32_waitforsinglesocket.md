# pgwin32_waitforsinglesocket

## Location
[src/backend/port/win32/socket.c:181-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/socket.c#L181-L290)

## Overview
pgwin32_waitforsinglesocket implements a Windows-specific socket waiting mechanism that efficiently waits for socket events while remaining responsive to PostgreSQL signals, with special handling for UDP socket write operations.

## Definition
```c
int pgwin32_waitforsinglesocket(SOCKET s, int what, int timeout)
```

## Detailed Description
This function provides a sophisticated socket waiting mechanism on Windows that addresses several platform-specific challenges. It uses Windows event objects and WSAEventSelect to wait for specified socket events while simultaneously monitoring for PostgreSQL signals. The function maintains static state to optimize performance across multiple calls, including a reusable event handle and socket type tracking.

A key feature is special UDP write handling that works around Windows locking issues under high load. When waiting for write events on UDP sockets, it implements a timeout-based retry mechanism with periodic WSASend attempts to detect if the socket becomes available.

The function ensures proper cleanup by detaching events from sockets before returning, allowing other code to attach different events to the same socket.

## Parameters / Member Variables
- `s`: The Windows socket to wait on
- `what`: Bit mask specifying socket events to wait for (FD_READ, FD_WRITE, etc.)
- `timeout`: Maximum wait time in milliseconds (-1 for infinite wait)
- `1`: Socket event occurred and is ready
- `0`: Error, timeout, or signal interruption (check errno for details)

## Dependencies
- Functions called/Symbols referenced:
  - CreateEvent (Windows API)
  - ResetEvent (Windows API)
  - WSAEventSelect (Winsock API)
  - WaitForMultipleObjectsEx (Windows API)
  - WSASend (Winsock API)
  - WSAGetLastError (Winsock API)
  - GetLastError (Windows API)
  - [isDataGram](../i/isDataGram.md) (socket type detection)
  - [TranslateSocketError](../T/TranslateSocketError.md) (error translation)
  - [pgwin32_dispatch_queued_signals](pgwin32_dispatch_queued_signals.md) (signal handling)
  - ereport (PostgreSQL error reporting)

- Called from (representative examples):
  - [pgwin32_connect](pgwin32_connect.md)
  - [pgwin32_recv](pgwin32_recv.md)
  - [pgwin32_send](pgwin32_send.md)

## Notes and Other Information
- This is a Windows-specific function located in src/backend/port/win32/socket.c
- Uses static variables to maintain state across calls for performance optimization
- Implements special workaround for UDP socket write operations under high load
- Simultaneously monitors both socket events and PostgreSQL signal events
- Sets appropriate errno values (EINTR for signals, EWOULDBLOCK for timeouts)
- Critical component of PostgreSQLs Windows socket abstraction layer
- Ensures signal responsiveness during potentially blocking socket operations
- Properly manages Windows event object lifecycle and socket event attachment/detachment