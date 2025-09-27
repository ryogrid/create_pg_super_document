# WaitEventAdjustPoll

## Location
[src/backend/storage/ipc/latch.c:1176-1220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L1176-L1220)

## Overview
Platform-specific function that configures poll file descriptor entries for wait events on systems using the traditional poll() mechanism.

## Definition
```c
static void WaitEventAdjustPoll(WaitEventSet *set, WaitEvent *event)
```

## Detailed Description  
This internal function handles the configuration of pollfd structures for wait events on systems that use the POSIX poll() system call. It translates PostgreSQL wait event types into corresponding poll event flags and updates the appropriate pollfd entry in the set's pollfds array.

The function maps PostgreSQL event types to poll flags as follows:
- **Latch events (WL_LATCH_SET)**: Sets POLLIN to wait for latch signal readability
- **Postmaster death events (WL_POSTMASTER_DEATH)**: Sets POLLIN to detect postmaster process termination
- **Socket events**: Converts WL_SOCKET_* flags to poll equivalents:
  - WL_SOCKET_READABLE → POLLIN
  - WL_SOCKET_WRITEABLE → POLLOUT
  - WL_SOCKET_CLOSED → POLLRDHUP (when available)

The function always clears the revents field and sets the file descriptor, ensuring the pollfd structure is ready for the next poll() call.

## Parameters / Member Variables
- `set`: Pointer to the WaitEventSet containing the pollfds array
- `event`: Pointer to the WaitEvent being configured

## Dependencies
- Functions called/Symbols referenced:
  - Assert (validation macro)
  - Standard poll flags (POLLIN, POLLOUT, POLLRDHUP)
- Called from (representative examples):
  - LatchWaitSetLatchPos
  - [AddWaitEventToSet](../A/AddWaitEventToSet.md)
  - [ModifyWaitEvent](../M/ModifyWaitEvent.md)

## Notes and Other Information
- Only compiled on systems with WAIT_USE_POLL defined (typically older Unix systems)
- Uses conditional compilation for POLLRDHUP support (not available on all systems)
- Directly updates the pollfd structure at the event's position index
- Always clears revents field to ensure clean state for next poll() operation
- Validates that all events have valid file descriptors
- Simpler than epoll/kqueue variants since poll() doesn't require separate registration calls
- Part of PostgreSQL's portable wait event system supporting multiple polling mechanisms

## Simplified Source

```c
// Simplified version of WaitEventAdjustPoll
static void WaitEventAdjustPoll(WaitEventSet *set, WaitEvent *event) {
    struct pollfd *pollfd = &set->pollfds[event->pos];

    // Clear previous results and set file descriptor
    pollfd->revents = 0;
    pollfd->fd = event->fd;

    // Map PostgreSQL event types to poll flags
    if (event->events == WL_LATCH_SET) {
        // Wait for latch signal
        pollfd->events = POLLIN;
    }
    else if (event->events == WL_POSTMASTER_DEATH) {
        // Wait for postmaster termination
        pollfd->events = POLLIN;
    }
    else {
        // Handle socket events
        pollfd->events = 0;
        if (event->events & WL_SOCKET_READABLE)
            pollfd->events |= POLLIN;
        if (event->events & WL_SOCKET_WRITEABLE)
            pollfd->events |= POLLOUT;
        if (event->events & WL_SOCKET_CLOSED)
            pollfd->events |= POLLRDHUP;  // Platform-dependent
    }
}
```

Key simplifications made:
- Removed detailed assertions for clarity (kept essential logic validation)
- Simplified conditional compilation directive explanations
- Consolidated socket event handling logic
- Added clear comments explaining the purpose of each major section
- Focused on the core event type to poll flag mapping functionality