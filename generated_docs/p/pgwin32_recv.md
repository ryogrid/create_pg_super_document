# pgwin32_recv

## Location
src/backend/port/win32/socket.c: 382 - 458

## Overview
PostgreSQL's Windows-specific wrapper function for receiving data from sockets that handles both blocking and non-blocking modes with robust signal integration and retry logic for Windows socket peculiarities.

## Definition
```c
int pgwin32_recv(SOCKET s, char *buf, int len, int f)
```

## Detailed Description
pgwin32_recv is the most complex of the Windows socket wrapper functions in PostgreSQL. It provides a complete replacement for the standard recv() function with sophisticated handling of both blocking and non-blocking operations. The function uses WSARecv() with WSABUF structures for enhanced Windows socket functionality. It includes signal polling, handles the pgwin32_noblock global flag for emulated non-blocking mode, and implements retry logic to work around Windows-specific socket behavior where WSARecv might return WSAEWOULDBLOCK even when the socket appears readable. The function includes up to 5 retry attempts with delays to handle these edge cases.

## Parameters / Member Variables
- `s`: Socket descriptor to receive data from
- `buf`: Buffer to store received data
- `len`: Maximum number of bytes to receive
- `f`: Flags for the receive operation (passed as WSA flags)

## Dependencies
- Functions called/Symbols referenced:
  - [pgwin32_poll_signals](pgwin32_poll_signals.md)
  - WSARecv (Windows socket API)
  - WSAGetLastError (Windows socket API)
  - [TranslateSocketError](../T/TranslateSocketError.md)
  - [pgwin32_waitforsinglesocket](pgwin32_waitforsinglesocket.md)
  - [pg_usleep](pg_usleep.md)
  - ereport
  - NOTICE
  - EWOULDBLOCK
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- This function is part of PostgreSQL's Windows socket abstraction layer located in src/backend/port/win32/socket.c
- Handles both blocking and emulated non-blocking modes via the global pgwin32_noblock flag
- Includes sophisticated retry logic (up to 5 attempts) to work around Windows socket implementation quirks
- Uses WSARecv() with WSABUF structures instead of standard recv() for better Windows integration
- Implements proper signal handling through pgwin32_poll_signals() at the beginning
- Waits for socket readiness using pgwin32_waitforsinglesocket() with FD_READ | FD_CLOSE | FD_ACCEPT events
- Includes a sleep-and-retry mechanism to handle cases where Windows reports a socket as ready but WSARecv still returns WSAEWOULDBLOCK
- Reports a NOTICE if all retry attempts fail, indicating a persistent Windows socket issue
- Returns the number of bytes received on success, -1 on error