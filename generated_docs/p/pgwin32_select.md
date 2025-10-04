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
- `nfds`: Maximum file descriptor number plus 1 (ignored on Windows, kept for POSIX compatibility)
- `*readfds`: Pointer to fd_set containing sockets to monitor for read readiness (input/output parameter)
- `*writefds`: Pointer to fd_set containing sockets to monitor for write readiness (input/output parameter)
- `*exceptfds`: Pointer to fd_set for exception conditions (must be NULL - not implemented)
- `*timeout`: Pointer to timeval structure specifying maximum wait time, or NULL for infinite wait
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

## Simplified Source

```c
int pgwin32_select(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timeval *timeout)
{
    WSAEVENT events[FD_SETSIZE * 2];
    SOCKET sockets[FD_SETSIZE * 2];
    int numevents = 0;
    int nummatches = 0;
    DWORD timeoutval = WSA_INFINITE;
    FD_SET outreadfds, outwritefds;

    Assert(exceptfds == NULL);

    // Check for pending signals first
    if (pgwin32_poll_signals())
        return -1;

    FD_ZERO(&outreadfds);
    FD_ZERO(&outwritefds);

    // Windows workaround: Test write readiness with dummy send
    if (writefds != NULL) {
        for (int i = 0; i < writefds->fd_count; i++) {
            char c;
            WSABUF buf = {0, &c};
            DWORD sent;

            if (WSASend(writefds->fd_array[i], &buf, 1, &sent, 0, NULL, NULL) == 0 ||
                WSAGetLastError() != WSAEWOULDBLOCK) {
                FD_SET(writefds->fd_array[i], &outwritefds);
            }
        }

        // Return immediately if any sockets are write-ready
        if (outwritefds.fd_count > 0) {
            memcpy(writefds, &outwritefds, sizeof(fd_set));
            if (readfds) FD_ZERO(readfds);
            return outwritefds.fd_count;
        }
    }

    // Convert timeout to milliseconds
    if (timeout != NULL) {
        timeoutval = timeout->tv_sec * 1000 + timeout->tv_usec / 1000;
    }

    // Create events for read sockets
    if (readfds != NULL) {
        for (int i = 0; i < readfds->fd_count; i++) {
            events[numevents] = WSACreateEvent();
            sockets[numevents] = readfds->fd_array[i];
            numevents++;
        }
    }

    // Create events for write sockets (avoiding duplicates)
    if (writefds != NULL) {
        for (int i = 0; i < writefds->fd_count; i++) {
            if (!readfds || !FD_ISSET(writefds->fd_array[i], readfds)) {
                events[numevents] = WSACreateEvent();
                sockets[numevents] = writefds->fd_array[i];
                numevents++;
            }
        }
    }

    // Associate sockets with events
    for (int i = 0; i < numevents; i++) {
        int flags = 0;
        if (readfds && FD_ISSET(sockets[i], readfds))
            flags |= FD_READ | FD_ACCEPT | FD_CLOSE;
        if (writefds && FD_ISSET(sockets[i], writefds))
            flags |= FD_WRITE | FD_CLOSE;

        if (WSAEventSelect(sockets[i], events[i], flags) != 0) {
            // Cleanup on error
            for (int j = 0; j < numevents; j++) {
                WSAEventSelect(sockets[j], NULL, 0);
                WSACloseEvent(events[j]);
            }
            TranslateSocketError();
            return -1;
        }
    }

    // Wait for events (including signal event)
    events[numevents] = pgwin32_signal_event;
    int result = WaitForMultipleObjectsEx(numevents + 1, events, FALSE, timeoutval, TRUE);

    // Process results if not timeout/signal
    if (result != WAIT_TIMEOUT && result != WAIT_IO_COMPLETION &&
        result != (WAIT_OBJECT_0 + numevents)) {

        // Check all sockets for activity
        for (int i = 0; i < numevents; i++) {
            WSANETWORKEVENTS resEvents;
            ZeroMemory(&resEvents, sizeof(resEvents));
            WSAEnumNetworkEvents(sockets[i], events[i], &resEvents);

            // Check read activity
            if (readfds && FD_ISSET(sockets[i], readfds) &&
                (resEvents.lNetworkEvents & (FD_READ | FD_ACCEPT | FD_CLOSE))) {
                FD_SET(sockets[i], &outreadfds);
                nummatches++;
            }

            // Check write activity
            if (writefds && FD_ISSET(sockets[i], writefds) &&
                (resEvents.lNetworkEvents & (FD_WRITE | FD_CLOSE))) {
                FD_SET(sockets[i], &outwritefds);
                nummatches++;
            }
        }
    }

    // Cleanup events
    for (int i = 0; i < numevents; i++) {
        WSAEventSelect(sockets[i], NULL, 0);
        WSACloseEvent(events[i]);
    }

    // Handle timeout
    if (result == WSA_WAIT_TIMEOUT) {
        if (readfds) FD_ZERO(readfds);
        if (writefds) FD_ZERO(writefds);
        return 0;
    }

    // Handle signals
    if (result == WAIT_OBJECT_0 + numevents || result == WAIT_IO_COMPLETION) {
        pgwin32_dispatch_queued_signals();
        errno = EINTR;
        if (readfds) FD_ZERO(readfds);
        if (writefds) FD_ZERO(writefds);
        return -1;
    }

    // Return results
    if (readfds) memcpy(readfds, &outreadfds, sizeof(fd_set));
    if (writefds) memcpy(writefds, &outwritefds, sizeof(fd_set));
    return nummatches;
}
```